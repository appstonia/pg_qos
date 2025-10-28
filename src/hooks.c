/*
 * hooks.c - PostgreSQL Quality of Service (QoS) Extension Hook Implementation
 *
 * This file implements the core hook registration and coordination for the 
 * PostgreSQL QoS extension. Actual implementations are in separate modules:
 * - hooks_cache.c: Cache management with syscache invalidation
 * - hooks_statement.c: Statement-level concurrent tracking
 * - hooks_transaction.c: Transaction-level concurrent tracking  
 * - hooks_resource.c: Resource limit enforcement (CPU, work_mem)
 *
 * Author:  M.Atif Ceylan
 * Company: AppstoniA OÜ
 * Created: October 02, 2025
 * Version: 1.0
 * License: See LICENSE file in the project root
 *
 * Copyright (c) 2025 AppstoniA OÜ
 * All rights reserved.
 */

#include "postgres.h"
#include "qos.h"
#include "hooks.h"
#include "hooks_internal.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "executor/executor.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"

/* Hook save variables */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static planner_hook_type prev_planner_hook = NULL;

/*
 * Planner hook - delegates to hooks_resource.c for CPU limit enforcement
 */
static PlannedStmt *
qos_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
    /* Delegate to hooks_resource.c for parallel worker adjustment */
    return qos_planner_hook(parse, query_string, cursorOptions, boundParams, prev_planner_hook);
}

/*
 * ProcessUtility hook - intercept SET commands
 */
static void
qos_ProcessUtility(PlannedStmt *pstmt,
                   const char *queryString,
                   bool readOnlyTree,
                   ProcessUtilityContext context,
                   ParamListInfo params,
                   QueryEnvironment *queryEnv,
                   DestReceiver *dest,
                   QueryCompletion *qc)
{
    Node *parsetree = pstmt->utilityStmt;
    bool bump_epoch_after = false;
    VariableSetStmt *qos_set = NULL;
    
    /* Check if setting work_mem - delegates to hooks_resource.c */
    if (qos_enabled && IsA(parsetree, VariableSetStmt))
    {
        VariableSetStmt *stmt = (VariableSetStmt *) parsetree;
        qos_enforce_work_mem_limit(stmt);
    }

    /* Detect ALTER ROLE/DB ... SET qos.* to bump global settings epoch */
    if (qos_enabled)
    {
        if (IsA(parsetree, AlterRoleSetStmt))
        {
            AlterRoleSetStmt *as = (AlterRoleSetStmt *) parsetree;
            qos_set = as->setstmt;
        }
        else if (IsA(parsetree, AlterDatabaseSetStmt))
        {
            AlterDatabaseSetStmt *as = (AlterDatabaseSetStmt *) parsetree;
            qos_set = as->setstmt;
        }

        if (qos_set && ((qos_set->name && pg_strncasecmp(qos_set->name, "qos.", 4) == 0) ||
                        qos_set->kind == VAR_RESET_ALL))
        {
            bump_epoch_after = true;
        }
    }
    
    /* Call previous hook or standard utility */
    if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                          params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                              params, queryEnv, dest, qc);

    /* If relevant QoS setting changed successfully, bump epoch so others reload */
    if (bump_epoch_after)
        qos_notify_settings_change();
}

/*
 * ExecutorStart hook - enforce limits and track query start
 */
static void
qos_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    /* Enforce CPU resource limits - delegates to hooks_resource.c */
    qos_enforce_cpu_limit();
    
    /* Track transaction if not already tracked - delegates to hooks_transaction.c */
    qos_track_transaction_start();
    
    /* Track statement-specific concurrency - delegates to hooks_statement.c */
    if (queryDesc->operation == CMD_SELECT || 
        queryDesc->operation == CMD_UPDATE || 
        queryDesc->operation == CMD_DELETE ||
        queryDesc->operation == CMD_INSERT)
    {
        qos_track_statement_start(queryDesc->operation);
    }
    
    /* Call previous hook or standard executor */
    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

/*
 * ExecutorEnd hook - track query completion
 */
static void
qos_ExecutorEnd(QueryDesc *queryDesc)
{
    /* Call previous hook or standard executor */
    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
    
    /* Decrement statement counter - delegates to hooks_statement.c */
    qos_track_statement_end();
    
    /* Decrement transaction counter - delegates to hooks_transaction.c */
    qos_track_transaction_end();
}

/*
 * Register all hooks and initialize cache system
 */
void
qos_register_hooks(void)
{
    /* Save previous hooks */
    prev_ProcessUtility = ProcessUtility_hook;
    prev_ExecutorStart = ExecutorStart_hook;
    prev_ExecutorEnd = ExecutorEnd_hook;
    prev_planner_hook = planner_hook;
    
    /* Install our hooks */
    ProcessUtility_hook = qos_ProcessUtility;
    ExecutorStart_hook = qos_ExecutorStart;
    ExecutorEnd_hook = qos_ExecutorEnd;
    planner_hook = qos_planner;
    
    /* Initialize cache system with syscache invalidation callbacks */
    qos_init_cache();
    
    elog(DEBUG1, "qos: hooks registered and cache initialized");
}

/*
 * Unregister all hooks
 */
void
qos_unregister_hooks(void)
{
    /* Restore previous hooks */
    ProcessUtility_hook = prev_ProcessUtility;
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook = prev_ExecutorEnd;
    planner_hook = prev_planner_hook;
    
    elog(DEBUG1, "qos: hooks unregistered");
}
