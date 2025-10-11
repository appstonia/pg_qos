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
#include "hooks.h"

#ifdef __linux__
#include <sched.h>
#endif

/* Hook save variables */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

/* Per-backend transaction tracking */
static bool transaction_tracked = false;
static bool cpu_affinity_set = false;

/*
 * Check and enforce resource limits for current session
 */
void
qos_check_and_enforce_limits(void)
{
    QoSLimits role_limits;
    QoSLimits db_limits;
    QoSLimits effective_limits;
    
    if (!qos_enabled)
        return;
    
    /* Get limits for role and database */
    role_limits = qos_get_role_limits(GetUserId());
    db_limits = qos_get_database_limits(MyDatabaseId);
    
    /* Effective limits are the minimum (most restrictive) of role and db */
    effective_limits.work_mem_limit = 
        (role_limits.work_mem_limit >= 0 && db_limits.work_mem_limit >= 0) ?
        Min(role_limits.work_mem_limit, db_limits.work_mem_limit) :
        Max(role_limits.work_mem_limit, db_limits.work_mem_limit);
        
    effective_limits.cpu_core_limit = 
        (role_limits.cpu_core_limit >= 0 && db_limits.cpu_core_limit >= 0) ?
        Min(role_limits.cpu_core_limit, db_limits.cpu_core_limit) :
        Max(role_limits.cpu_core_limit, db_limits.cpu_core_limit);
        
    effective_limits.max_concurrent_tx = 
        (role_limits.max_concurrent_tx >= 0 && db_limits.max_concurrent_tx >= 0) ?
        Min(role_limits.max_concurrent_tx, db_limits.max_concurrent_tx) :
        Max(role_limits.max_concurrent_tx, db_limits.max_concurrent_tx);
    
    /* Enforce CPU core limit using CPU affinity (Linux only) */
    if (effective_limits.cpu_core_limit > 0 && !cpu_affinity_set)
    {
#ifdef __linux__
        cpu_set_t cpuset;
        int i;
        
        CPU_ZERO(&cpuset);
        /* Set affinity to first N cores */
        for (i = 0; i < effective_limits.cpu_core_limit && i < CPU_SETSIZE; i++)
        {
            CPU_SET(i, &cpuset);
        }
        
        if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) == 0)
        {
            cpu_affinity_set = true;
            elog(DEBUG1, "qos: limited user %u to %d CPU cores",
                 GetUserId(), effective_limits.cpu_core_limit);
        }
        else
        {
            elog(WARNING, "qos: failed to set CPU affinity for user %u: %m",
                 GetUserId());
        }
#else
        elog(DEBUG1, "qos: CPU core limiting not supported on this platform");
#endif
    }
}

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
 * Track transaction start
 */
void
qos_track_transaction_start(void)
{
    QoSLimits role_limits;
    QoSLimits db_limits;
    int max_concurrent = -1;
    
    if (!qos_enabled || transaction_tracked)
        return;
    
    role_limits = qos_get_role_limits(GetUserId());
    db_limits = qos_get_database_limits(MyDatabaseId);
    
    /* Get most restrictive concurrent transaction limit */
    if (role_limits.max_concurrent_tx >= 0 && db_limits.max_concurrent_tx >= 0)
        max_concurrent = Min(role_limits.max_concurrent_tx, db_limits.max_concurrent_tx);
    else if (role_limits.max_concurrent_tx >= 0)
        max_concurrent = role_limits.max_concurrent_tx;
    else if (db_limits.max_concurrent_tx >= 0)
        max_concurrent = db_limits.max_concurrent_tx;    
    
    if (qos_shared_state && max_concurrent > 0)
    {
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        
        if (qos_shared_state->active_transactions >= max_concurrent)
        {
            qos_shared_state->stats.concurrent_tx_violations++;
            qos_shared_state->stats.rejected_queries++;
            LWLockRelease(qos_shared_state->lock);
            
            ereport(ERROR,
                    (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                     errmsg("qos: maximum concurrent transactions exceeded"),
                     errdetail("Current: %d, Maximum: %d",
                              qos_shared_state->active_transactions, max_concurrent),
                     errhint("Wait for other transactions to complete")));
        }
        
        qos_shared_state->active_transactions++;
        qos_shared_state->stats.total_queries++;
        transaction_tracked = true;
        
        LWLockRelease(qos_shared_state->lock);
    }
}

/*
 * Track transaction end
 */
void
qos_track_transaction_end(void)
{
    if (!qos_enabled || !transaction_tracked)
        return;
    
    if (qos_shared_state)
    {
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        
        if (qos_shared_state->active_transactions > 0)
            qos_shared_state->active_transactions--;
        
        LWLockRelease(qos_shared_state->lock);
    }
    
    transaction_tracked = false;
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
 * ExecutorStart hook - track transaction start and enforce limits
 */
static void
qos_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    /* Check and enforce limits at query start */
    qos_check_and_enforce_limits();
    
    /* Track transaction if not already tracked */
    qos_track_transaction_start();
    
    /* Call previous hook or standard executor */
    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

/*
 * ExecutorEnd hook - track transaction end
 */
static void
qos_ExecutorEnd(QueryDesc *queryDesc)
{
    /* Call previous hook or standard executor */
    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
    
    /* Decrement transaction counter when query completes */
    qos_track_transaction_end();
}

/*
 * Register all hooks
 */
void
qos_register_hooks(void)
{
    /* Save previous hooks */
    prev_ProcessUtility = ProcessUtility_hook;
    prev_ExecutorStart = ExecutorStart_hook;
    prev_ExecutorEnd = ExecutorEnd_hook;
    
    /* Install our hooks */
    ProcessUtility_hook = qos_ProcessUtility;
    ExecutorStart_hook = qos_ExecutorStart;
    ExecutorEnd_hook = qos_ExecutorEnd;
    
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
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook = prev_ExecutorEnd;
    
    elog(DEBUG1, "qos: hooks unregistered");
}