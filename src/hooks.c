/*
 * hooks.c - PostgreSQL Quality of Service (QoS) Extension Hook Implementation
 *
 * This file implements the core hook functionality for the PostgreSQL QoS extension,
 * providing query execution control and resource management through PostgreSQL's
 * hook mechanism.
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
#include "miscadmin.h"
#include "tcop/utility.h"
#include "executor/executor.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "storage/proc.h"
#include "portability/instr_time.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include <unistd.h>
#include <sys/resource.h>
#include <strings.h>

/* Hook save variables */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/*
 * Enforce work_mem limit when SET work_mem is executed
 */
void
qos_enforce_work_mem_limit(VariableSetStmt *stmt)
{
    QoSLimits role_limits;
    QoSLimits db_limits;
    int64 max_work_mem = -1;
    int new_work_mem_kb = 0;
    A_Const *arg;
    char *value_str = NULL;
    char *endptr;
    int64 value;
    int64 new_work_mem_bytes;
    
    if (!qos_enabled)
        return;
    
    /* Only process work_mem SET commands */
    if (!stmt || !stmt->name || strcmp(stmt->name, "work_mem") != 0 || stmt->kind != VAR_SET_VALUE)
        return;
    
    role_limits = qos_get_role_limits(GetUserId());
    db_limits = qos_get_database_limits(MyDatabaseId);
    
    /* Get most restrictive work_mem limit */
    if (role_limits.work_mem_limit >= 0 && db_limits.work_mem_limit >= 0)
        max_work_mem = Min(role_limits.work_mem_limit, db_limits.work_mem_limit);
    else if (role_limits.work_mem_limit >= 0)
        max_work_mem = role_limits.work_mem_limit;
    else if (db_limits.work_mem_limit >= 0)
        max_work_mem = db_limits.work_mem_limit;
    
    /* Parse the new value */
    if (max_work_mem >= 0 && stmt->args != NIL)
    {
        arg = (A_Const *) linitial(stmt->args);
        
        if (IsA(arg, A_Const))
        {
            if (nodeTag(&arg->val) == T_Integer)
            {
                new_work_mem_kb = intVal(&arg->val);
            }
            else if (nodeTag(&arg->val) == T_String)
            {
                value_str = strVal(&arg->val);
                /* Parse memory value like "128MB" */
                value = strtol(value_str, &endptr, 10);
                
                if (*endptr != '\0')
                {
                    /* Has unit suffix */
                    if (strcasecmp(endptr, "kb") == 0 || strcasecmp(endptr, "kB") == 0)
                        new_work_mem_kb = value;
                    else if (strcasecmp(endptr, "mb") == 0 || strcasecmp(endptr, "MB") == 0)
                        new_work_mem_kb = value * 1024;
                    else if (strcasecmp(endptr, "gb") == 0 || strcasecmp(endptr, "GB") == 0)
                        new_work_mem_kb = value * 1024 * 1024;
                    else
                        new_work_mem_kb = value; /* Assume KB */
                }
                else
                {
                    new_work_mem_kb = value; /* No unit = KB */
                }
            }
        }
        
        /* Check limit */
        new_work_mem_bytes = (int64)new_work_mem_kb * 1024L;
        
        if (new_work_mem_bytes > max_work_mem)
        {
            if (qos_shared_state)
            {
                LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
                qos_shared_state->stats.work_mem_violations++;
                LWLockRelease(qos_shared_state->lock);
            }
            
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                     errmsg("qos: work_mem limit exceeded"),
                     errdetail("Requested %d KB, maximum allowed is %ld KB",
                              new_work_mem_kb, max_work_mem / 1024),
                     errhint("Contact administrator to increase qos.work_mem_limit")));
        }
    }
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
    
    /* Check if setting work_mem */
    if (qos_enabled && IsA(parsetree, VariableSetStmt))
    {
        VariableSetStmt *stmt = (VariableSetStmt *) parsetree;
        qos_enforce_work_mem_limit(stmt);
    }
    
    /* Call previous hook or standard utility */
    if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                          params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                              params, queryEnv, dest, qc);
}

/*
 * Register all hooks
 */
void
qos_register_hooks(void)
{
    /* Save previous hooks */
    prev_ProcessUtility = ProcessUtility_hook;    
    
    /* Install our hooks */
    ProcessUtility_hook = qos_ProcessUtility;
        
    elog(DEBUG1, "qos: hooks registered");
}

/*
 * Unregister all hooks
 */
void
qos_unregister_hooks(void)
{
    /* Restore previous hooks */
    ProcessUtility_hook = prev_ProcessUtility;    
    
    elog(DEBUG1, "qos: hooks unregistered");
}