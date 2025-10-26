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
#include "access/xact.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
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
static CmdType current_statement_type = CMD_UNKNOWN;
static bool statement_tracked = false;

/* Cached QoS limits - invalidated via syscache callback */
static QoSLimits cached_limits = {-1, -1, -1, -1, -1, -1, -1, -1, -1};
static Oid cached_user_id = InvalidOid;
static Oid cached_db_id = InvalidOid;
static bool limits_cached = false;

/*
 * Invalidation callback for pg_db_role_setting changes
 * This is called automatically when ALTER ROLE/DATABASE changes settings
 */
static void
qos_invalidate_cache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
    /* Invalidate cache when database or role settings change */
    if (cacheid == DATABASEOID || cacheid == AUTHOID)
    {
        limits_cached = false;
        elog(DEBUG2, "qos: cache invalidated due to role/database configuration change");
    }
}

/*
 * Initialize or refresh cached limits for current session
 * Cache is automatically invalidated via syscache callback when configs change
 */
static void
qos_refresh_cached_limits(void)
{
    Oid current_user_id;
    Oid current_db_id;
    QoSLimits role_limits;
    QoSLimits db_limits;
    
    current_user_id = GetUserId();
    current_db_id = MyDatabaseId;
    
    /* Check if cache is still valid */
    if (limits_cached && 
        cached_user_id == current_user_id && 
        cached_db_id == current_db_id)
    {
        return; /* Cache is valid */
    }
    
    /* Refresh cache - query catalogs */
    role_limits = qos_get_role_limits(current_user_id);
    db_limits = qos_get_database_limits(current_db_id);
    
    /* Calculate limits (most restrictive) */
    // work_mem_limit
    if (role_limits.work_mem_limit >= 0 && db_limits.work_mem_limit >= 0)
        cached_limits.work_mem_limit = Min(role_limits.work_mem_limit, db_limits.work_mem_limit);
    else if (role_limits.work_mem_limit >= 0)
        cached_limits.work_mem_limit = role_limits.work_mem_limit;
    else if (db_limits.work_mem_limit >= 0)
        cached_limits.work_mem_limit = db_limits.work_mem_limit;
    else
        cached_limits.work_mem_limit = -1;
    
    // cpu_core_limit
    if (role_limits.cpu_core_limit >= 0 && db_limits.cpu_core_limit >= 0)
        cached_limits.cpu_core_limit = Min(role_limits.cpu_core_limit, db_limits.cpu_core_limit);
    else if (role_limits.cpu_core_limit >= 0)
        cached_limits.cpu_core_limit = role_limits.cpu_core_limit;
    else if (db_limits.cpu_core_limit >= 0)
        cached_limits.cpu_core_limit = db_limits.cpu_core_limit;
    else
        cached_limits.cpu_core_limit = -1;
    
    // max_concurrent_tx
    if (role_limits.max_concurrent_tx >= 0 && db_limits.max_concurrent_tx >= 0)
        cached_limits.max_concurrent_tx = Min(role_limits.max_concurrent_tx, db_limits.max_concurrent_tx);
    else if (role_limits.max_concurrent_tx >= 0)
        cached_limits.max_concurrent_tx = role_limits.max_concurrent_tx;
    else if (db_limits.max_concurrent_tx >= 0)
        cached_limits.max_concurrent_tx = db_limits.max_concurrent_tx;
    else
        cached_limits.max_concurrent_tx = -1;
    
    /* Statement-specific concurrent limits */
    // max_concurrent_select
    if (role_limits.max_concurrent_select >= 0 && db_limits.max_concurrent_select >= 0)
        cached_limits.max_concurrent_select = Min(role_limits.max_concurrent_select, db_limits.max_concurrent_select);
    else if (role_limits.max_concurrent_select >= 0)
        cached_limits.max_concurrent_select = role_limits.max_concurrent_select;
    else if (db_limits.max_concurrent_select >= 0)
        cached_limits.max_concurrent_select = db_limits.max_concurrent_select;
    else
        cached_limits.max_concurrent_select = -1;
        
    // max_concurrent_update    
    if (role_limits.max_concurrent_update >= 0 && db_limits.max_concurrent_update >= 0)
        cached_limits.max_concurrent_update = Min(role_limits.max_concurrent_update, db_limits.max_concurrent_update);
    else if (role_limits.max_concurrent_update >= 0)
        cached_limits.max_concurrent_update = role_limits.max_concurrent_update;
    else if (db_limits.max_concurrent_update >= 0)
        cached_limits.max_concurrent_update = db_limits.max_concurrent_update;
    else
        cached_limits.max_concurrent_update = -1;
    
    // max_concurrent_delete
    if (role_limits.max_concurrent_delete >= 0 && db_limits.max_concurrent_delete >= 0)
        cached_limits.max_concurrent_delete = Min(role_limits.max_concurrent_delete, db_limits.max_concurrent_delete);
    else if (role_limits.max_concurrent_delete >= 0)
        cached_limits.max_concurrent_delete = role_limits.max_concurrent_delete;
    else if (db_limits.max_concurrent_delete >= 0)
        cached_limits.max_concurrent_delete = db_limits.max_concurrent_delete;
    else
        cached_limits.max_concurrent_delete = -1;
    
    // max_concurrent_insert
    if (role_limits.max_concurrent_insert >= 0 && db_limits.max_concurrent_insert >= 0)
        cached_limits.max_concurrent_insert = Min(role_limits.max_concurrent_insert, db_limits.max_concurrent_insert);
    else if (role_limits.max_concurrent_insert >= 0)
        cached_limits.max_concurrent_insert = role_limits.max_concurrent_insert;
    else if (db_limits.max_concurrent_insert >= 0)
        cached_limits.max_concurrent_insert = db_limits.max_concurrent_insert;
    else
        cached_limits.max_concurrent_insert = -1;

    /* Update cache metadata for each role and database*/
    cached_user_id = current_user_id;
    cached_db_id = current_db_id;
    limits_cached = true;
    
    elog(DEBUG2, "qos: cached limits refreshed - work_mem: %ld, cpu_cores: %d, max_tx: %d (user: %u, db: %u)",
         cached_limits.work_mem_limit,
         cached_limits.cpu_core_limit,
         cached_limits.max_concurrent_tx,
         cached_user_id,
         cached_db_id);
}

/*
 * Get cached effective limits (refreshes if needed)
 */
static inline QoSLimits
qos_get_cached_limits(void)
{
    qos_refresh_cached_limits();
    return cached_limits;
}

/*
 * Check and enforce resource limits for current session
 */
void
qos_enforce_cpu_limit(void)
{
    QoSLimits limits;
    
    if (!qos_enabled)
        return;
    
    /* Get cached limits - no catalog access needed */
    limits = qos_get_cached_limits();
    
    /* Enforce CPU core limit using CPU affinity (Linux only) */
    if (limits.cpu_core_limit > 0 && !cpu_affinity_set)
    {
        //TODO: Add support for other OSes
#ifdef __linux__
        cpu_set_t cpuset;
        int i;
        
        CPU_ZERO(&cpuset);
        /* Set affinity to first N cores */
        for (i = 0; i < limits.cpu_core_limit && i < CPU_SETSIZE; i++)
        {
            CPU_SET(i, &cpuset);
        }
        
        if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) == 0)
        {
            cpu_affinity_set = true;
            elog(DEBUG1, "qos: limited user %u to %d CPU cores",
                 GetUserId(), limits.cpu_core_limit);
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
    QoSLimits limits;
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
    
    /* Get cached limits - no catalog access needed */
    limits = qos_get_cached_limits();
    
    /* Parse the new value */
    if (limits.work_mem_limit >= 0 && stmt->args != NIL)
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
        
        if (new_work_mem_bytes > limits.work_mem_limit)
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
                              new_work_mem_kb, limits.work_mem_limit / 1024),
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
    QoSLimits limits;
    
    if (!qos_enabled || transaction_tracked)
        return;
    
    /* Get cached limits - no catalog access needed */
    limits = qos_get_cached_limits();
    
    if (qos_shared_state && limits.max_concurrent_tx > 0)
    {
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        
        if (qos_shared_state->active_transactions >= limits.max_concurrent_tx)
        {
            qos_shared_state->stats.concurrent_tx_violations++;
            qos_shared_state->stats.rejected_queries++;
            LWLockRelease(qos_shared_state->lock);
            
            ereport(ERROR,
                    (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                     errmsg("qos: maximum concurrent transactions exceeded"),
                     errdetail("Current: %d, Maximum: %d",
                              qos_shared_state->active_transactions, limits.max_concurrent_tx),
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
 * Track statement start - for SELECT, UPDATE, DELETE, INSERT concurrency limits
 */
static void
qos_track_statement_start(CmdType operation)
{
    QoSLimits limits;
    
    if (!qos_enabled || statement_tracked)
        return;
    
    /* Only track SELECT, UPDATE, DELETE, INSERT */
    if (operation != CMD_SELECT && operation != CMD_UPDATE && 
        operation != CMD_DELETE && operation != CMD_INSERT)
        return;
    
    limits = qos_get_cached_limits();
    
    if (qos_shared_state)
    {
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        
        /* Check and enforce statement-specific concurrent limits */
        switch (operation)
        {
            case CMD_SELECT:
                if (limits.max_concurrent_select > 0 && 
                    qos_shared_state->active_selects >= limits.max_concurrent_select)
                {
                    qos_shared_state->stats.concurrent_select_violations++;
                    qos_shared_state->stats.rejected_queries++;
                    LWLockRelease(qos_shared_state->lock);
                    
                    ereport(ERROR,
                            (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                             errmsg("qos: maximum concurrent SELECT statements exceeded"),
                             errdetail("Current: %d, Maximum: %d",
                                      qos_shared_state->active_selects, limits.max_concurrent_select),
                             errhint("Wait for other SELECT queries to complete")));
                }
                qos_shared_state->active_selects++;
                break;
                
            case CMD_UPDATE:
                if (limits.max_concurrent_update > 0 && 
                    qos_shared_state->active_updates >= limits.max_concurrent_update)
                {
                    qos_shared_state->stats.concurrent_update_violations++;
                    qos_shared_state->stats.rejected_queries++;
                    LWLockRelease(qos_shared_state->lock);
                    
                    ereport(ERROR,
                            (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                             errmsg("qos: maximum concurrent UPDATE statements exceeded"),
                             errdetail("Current: %d, Maximum: %d",
                                      qos_shared_state->active_updates, limits.max_concurrent_update),
                             errhint("Wait for other UPDATE queries to complete")));
                }
                qos_shared_state->active_updates++;
                break;
                
            case CMD_DELETE:
                if (limits.max_concurrent_delete > 0 && 
                    qos_shared_state->active_deletes >= limits.max_concurrent_delete)
                {
                    qos_shared_state->stats.concurrent_delete_violations++;
                    qos_shared_state->stats.rejected_queries++;
                    LWLockRelease(qos_shared_state->lock);
                    
                    ereport(ERROR,
                            (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                             errmsg("qos: maximum concurrent DELETE statements exceeded"),
                             errdetail("Current: %d, Maximum: %d",
                                      qos_shared_state->active_deletes, limits.max_concurrent_delete),
                             errhint("Wait for other DELETE queries to complete")));
                }
                qos_shared_state->active_deletes++;
                break;
                
            case CMD_INSERT:
                if (limits.max_concurrent_insert > 0 && 
                    qos_shared_state->active_inserts >= limits.max_concurrent_insert)
                {
                    qos_shared_state->stats.concurrent_insert_violations++;
                    qos_shared_state->stats.rejected_queries++;
                    LWLockRelease(qos_shared_state->lock);
                    
                    ereport(ERROR,
                            (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                             errmsg("qos: maximum concurrent INSERT statements exceeded"),
                             errdetail("Current: %d, Maximum: %d",
                                      qos_shared_state->active_inserts, limits.max_concurrent_insert),
                             errhint("Wait for other INSERT queries to complete")));
                }
                qos_shared_state->active_inserts++;
                break;
                
            default:
                break;
        }
        
        current_statement_type = operation;
        statement_tracked = true;
        
        LWLockRelease(qos_shared_state->lock);
    }
}

/*
 * Track statement end - decrement statement-specific counters
 */
static void
qos_track_statement_end(void)
{
    if (!qos_enabled || !statement_tracked)
        return;
    
    if (qos_shared_state)
    {
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        
        switch (current_statement_type)
        {
            case CMD_SELECT:
                if (qos_shared_state->active_selects > 0)
                    qos_shared_state->active_selects--;
                break;
                
            case CMD_UPDATE:
                if (qos_shared_state->active_updates > 0)
                    qos_shared_state->active_updates--;
                break;
                
            case CMD_DELETE:
                if (qos_shared_state->active_deletes > 0)
                    qos_shared_state->active_deletes--;
                break;
                
            case CMD_INSERT:
                if (qos_shared_state->active_inserts > 0)
                    qos_shared_state->active_inserts--;
                break;
                
            default:
                break;
        }
        
        LWLockRelease(qos_shared_state->lock);
    }
    
    statement_tracked = false;
    current_statement_type = CMD_UNKNOWN;
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
    qos_enforce_cpu_limit();
    
    /* Track transaction if not already tracked */
    qos_track_transaction_start();
    
    /* Track statement-specific concurrency (SELECT, UPDATE, DELETE, INSERT) */
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
 * ExecutorEnd hook - track transaction end and statement end
 */
static void
qos_ExecutorEnd(QueryDesc *queryDesc)
{
    /* Call previous hook or standard executor */
    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
    
    /* Decrement statement counter */
    qos_track_statement_end();
    
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
    
    /* Register syscache invalidation callbacks for role and database changes */
    CacheRegisterSyscacheCallback(DATABASEOID, qos_invalidate_cache_callback, (Datum) 0);
    CacheRegisterSyscacheCallback(AUTHOID, qos_invalidate_cache_callback, (Datum) 0);
    
    elog(DEBUG1, "qos: hooks and cache callbacks registered");
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