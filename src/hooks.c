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
#include "commands/defrem.h"
#include "access/xact.h"
#include "nodes/makefuncs.h"
#include "parser/parse_node.h"
#include "nodes/value.h"
#include <ctype.h>

/* Hook save variables */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static planner_hook_type prev_planner_hook = NULL;

/* Flag to suppress concurrency tracking in planner (for EXPLAIN/PREPARE) */
static bool suppress_concurrency_tracking = false;

static void qos_validate_qos_setstmt(VariableSetStmt *stmt);
static char *qos_normalize_work_mem_value(const char *value_str);
#if PG_VERSION_NUM >= 170000
static A_Const *qos_make_string_const(const char *str, int location);
#endif

/* For version 17+ */
#ifndef MyBackendId
static int qos_backend_slot = -1;

int
qos_get_backend_slot(bool allocate_if_missing)
{
    int slot;
    int i;

    if (!qos_shared_state)
        return -1;

    slot = qos_backend_slot;
    if (slot >= 0 && qos_shared_state->backend_status[slot].pid == MyProcPid)
        return slot;

    LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);

    slot = qos_backend_slot;
    if (slot >= 0 && qos_shared_state->backend_status[slot].pid == MyProcPid)
    {
        LWLockRelease(qos_shared_state->lock);
        return slot;
    }

    for (i = 0; i < qos_shared_state->max_backends; i++)
    {
        if (qos_shared_state->backend_status[i].pid == MyProcPid)
        {
            slot = i;
            break;
        }
    }

    if (slot < 0 && allocate_if_missing)
    {
        for (i = 0; i < qos_shared_state->max_backends; i++)
        {
            if (qos_shared_state->backend_status[i].pid == 0)
            {
                qos_shared_state->backend_status[i].pid = MyProcPid;
                slot = i;
                break;
            }
        }
    }

    qos_backend_slot = slot;

    LWLockRelease(qos_shared_state->lock);

    return slot;
}

void
qos_reset_backend_slot(void)
{
    qos_backend_slot = -1;
}
#endif /* MyBackendId */

/*
 * Transaction callback to handle cleanup on abort/error
 */
static void
qos_xact_callback(XactEvent event, void *arg)
{
    /* 
     * If transaction aborts (error, cancel, disconnect), 
     * ensure we decrement counters if they were tracked.
     * ExecutorEnd is not called on error, so we need this safety net.
     */
    if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
    {
        elog(DEBUG3, "qos: transaction callback called on abort, cleaning up concurrency tracking");
        qos_track_statement_end();
        qos_track_transaction_end();
    }
}

/*
 * Planner hook - delegates to hooks_resource.c for CPU limit enforcement
 */
static PlannedStmt *
qos_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
    /* Enforce work_mem limit BEFORE query planning starts */
    if (qos_enabled)
    {
        elog(DEBUG3, "qos: planner_hook called, enforcing work_mem");
        qos_enforce_work_mem_limit(NULL);
    }
    
    /* 
     * Track concurrency limits BEFORE planning to avoid overhead of rejected queries.
     * We skip this for EXPLAIN (no analyze) and PREPARE to avoid leaking counts.
     * EXECUTE statements will be caught by ExecutorStart hook.
     */
    if (qos_enabled && !suppress_concurrency_tracking)
    {
        /* Track transaction */
        qos_track_transaction_start();
        
        /* Track statement */
        if (parse->commandType == CMD_SELECT || 
            parse->commandType == CMD_UPDATE || 
            parse->commandType == CMD_DELETE || 
            parse->commandType == CMD_INSERT)
        {
            qos_track_statement_start(parse->commandType);
        }
    }
    
    /* Delegate to hooks_resource.c for parallel worker adjustment */
    return qos_planner_hook(parse, query_string, cursorOptions, boundParams, prev_planner_hook);
}

static char *
qos_normalize_work_mem_value(const char *value_str)
{
    const char *ptr;
    const char *num_start;
    const char *unit_start;
    size_t num_len;
    char *num_part;
    char *unit_part;
    const char *normalized_unit = NULL;

    if (value_str == NULL)
        return NULL;

    ptr = value_str;
    while (*ptr != '\0' && isspace((unsigned char) *ptr))
        ptr++;

    num_start = ptr;
    while (*ptr != '\0' && isdigit((unsigned char) *ptr))
        ptr++;

    if (ptr == num_start)
        return NULL;

    num_len = (size_t) (ptr - num_start);
    num_part = palloc(num_len + 1);
    memcpy(num_part, num_start, num_len);
    num_part[num_len] = '\0';

    while (*ptr != '\0' && isspace((unsigned char) *ptr))
        ptr++;

    unit_start = ptr;
    while (*ptr != '\0' && isalpha((unsigned char) *ptr))
        ptr++;

    if (*ptr != '\0')
    {
        pfree(num_part);
        return NULL;
    }

    if (unit_start == ptr)
    {
        normalized_unit = "MB";
    }
    else
    {
        unit_part = pnstrdup(unit_start, ptr - unit_start);
        if (pg_strcasecmp(unit_part, "k") == 0 || pg_strcasecmp(unit_part, "kb") == 0)
            normalized_unit = "kB";
        else if (pg_strcasecmp(unit_part, "m") == 0 || pg_strcasecmp(unit_part, "mb") == 0)
            normalized_unit = "MB";
        else if (pg_strcasecmp(unit_part, "g") == 0 || pg_strcasecmp(unit_part, "gb") == 0)
            normalized_unit = "GB";
        pfree(unit_part);
    }

    if (!normalized_unit)
    {
        pfree(num_part);
        return NULL;
    }

    {
        char *result = psprintf("%s%s", num_part, normalized_unit);
        pfree(num_part);
        return result;
    }
}

static void
qos_validate_qos_setstmt(VariableSetStmt *stmt)
{
    const char *value_str = NULL;
    char *formatted_value = NULL;
    Node *arg;

    if (!stmt || !stmt->name)
        return;

    if (pg_strncasecmp(stmt->name, "qos.", 4) != 0)
        return;

    if (!qos_is_valid_qos_param_name(stmt->name))
    {
        ereport(ERROR,
                (errmsg("qos: invalid parameter name \"%s\"", stmt->name),
                 errhint("Valid parameters: qos.work_mem_limit, qos.cpu_core_limit, qos.max_concurrent_tx, "
                         "qos.max_concurrent_select, qos.max_concurrent_update, qos.max_concurrent_delete, "
                         "qos.max_concurrent_insert")));
    }

    if (strcmp(stmt->name, "qos.enabled") == 0)
        return;

    switch (stmt->kind)
    {
        case VAR_SET_VALUE:
            if (stmt->args == NIL)
                ereport(ERROR,
                        (errmsg("qos: missing value for parameter \"%s\"", stmt->name)));

            arg = (Node *) linitial(stmt->args);
            if (!IsA(arg, A_Const))
                ereport(ERROR,
                        (errmsg("qos: invalid value for parameter \"%s\"", stmt->name)));

            if (nodeTag(&((A_Const *) arg)->val) == T_Integer)
            {
                formatted_value = psprintf("%ld", (long) intVal(&((A_Const *) arg)->val));
                value_str = formatted_value;
            }
            else if (nodeTag(&((A_Const *) arg)->val) == T_Float)
            {
                value_str = strVal(&((A_Const *) arg)->val);
            }
            else if (nodeTag(&((A_Const *) arg)->val) == T_String)
            {
                value_str = strVal(&((A_Const *) arg)->val);
            }
            else
            {
                ereport(ERROR,
                        (errmsg("qos: invalid value for parameter \"%s\"", stmt->name)));
            }

            if (strcmp(stmt->name, "qos.work_mem_limit") == 0 && value_str != NULL)
            {
                char *normalized = qos_normalize_work_mem_value(value_str);

                if (normalized && strcmp(normalized, value_str) != 0)
                {
#if PG_VERSION_NUM >= 170000
                    A_Const *newconst = qos_make_string_const(normalized, -1);

                    stmt->args = list_make1(newconst);
                    value_str = strVal(&newconst->val);
#else
                    value_str = normalized;
#endif
                }

                qos_apply_qos_param_value(NULL, stmt->name, value_str, true);

                if (normalized && normalized != value_str)
                    pfree(normalized);
            }
            else
            {
                qos_apply_qos_param_value(NULL, stmt->name, value_str, true);
            }
            if (formatted_value)
                pfree(formatted_value);
            break;

        case VAR_SET_DEFAULT:
        case VAR_SET_CURRENT:
        case VAR_RESET:
        case VAR_RESET_ALL:
            break;

        default:
            ereport(ERROR,
                    (errmsg("qos: unsupported SET option for parameter \"%s\"", stmt->name)));
    }
}

#if PG_VERSION_NUM >= 170000
static A_Const *
qos_make_string_const(const char *str, int location)
{
    return (A_Const *) makeStringConst(pstrdup(str), location);
}
#endif

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
    
    /* Enforce work_mem limit on first command in session - delegates to hooks_resource.c */
    if (qos_enabled)
    {
        elog(DEBUG2, "qos: ProcessUtility hook called, enforcing work_mem");
        qos_enforce_work_mem_limit(NULL);
    }
    
    /* Check if setting work_mem - delegates to hooks_resource.c */
    if (qos_enabled && IsA(parsetree, VariableSetStmt))
    {
        VariableSetStmt *stmt = (VariableSetStmt *) parsetree;
        qos_enforce_work_mem_limit(stmt);
    }

    /* Validate qos.* input and detect ALTER ROLE/DB ... SET qos.* */
    if (qos_enabled)
    {
        if (IsA(parsetree, VariableSetStmt))
            qos_validate_qos_setstmt((VariableSetStmt *) parsetree);

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

        if (qos_set)
            qos_validate_qos_setstmt(qos_set);

        if (qos_set && ((qos_set->name && pg_strncasecmp(qos_set->name, "qos.", 4) == 0) ||
                        qos_set->kind == VAR_RESET_ALL))
        {
            bump_epoch_after = true;
        }
        
        /* 
         * Suppress concurrency tracking in planner for EXPLAIN (without ANALYZE) 
         * and PREPARE statements to prevent counter leaks.
         */
        if (IsA(parsetree, ExplainStmt))
        {
            ExplainStmt *estmt = (ExplainStmt *) parsetree;
            bool is_analyze = false;
            ListCell *lc;
            
            foreach(lc, estmt->options)
            {
                DefElem *opt = (DefElem *) lfirst(lc);
                if (strcmp(opt->defname, "analyze") == 0)
                {
                    is_analyze = true;
                    if (opt->arg != NULL)
                        is_analyze = defGetBoolean(opt);
                }
            }
            
            if (!is_analyze)
                suppress_concurrency_tracking = true;
        }
        else if (IsA(parsetree, PrepareStmt))
        {
            suppress_concurrency_tracking = true;
        }
    }
    
    /* Call previous hook or standard utility */
    if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                          params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                              params, queryEnv, dest, qc);

    /* Reset suppression flag */
    suppress_concurrency_tracking = false;

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
    
    /* 
     * Note: Concurrency tracking is now primarily handled in qos_planner 
     * to reject queries before planning overhead.
     * 
     * However, we still call these here to handle cases where planner hook 
     * wasn't called or tracking was suppressed (e.g. EXECUTE of prepared stmt).
     * The tracking functions have internal flags (statement_tracked/transaction_tracked)
     * to ensure we don't double-count.
     */
    
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
    
    /* Register transaction callback for cleanup on abort */
    RegisterXactCallback(qos_xact_callback, NULL);
    
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
