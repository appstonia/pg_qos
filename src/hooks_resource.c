/*
 * hooks_resource.c - Resource limit enforcement
 *
 * This file implements CPU and memory resource limit enforcement.
 *
 * Author:  M.Atif Ceylan
 * Company: AppstoniA OÜ
 * Created: October 28, 2025
 * Version: 1.0
 * License: See LICENSE file in the project root
 *
 * Copyright (c) 2025 AppstoniA OÜ
 * All rights reserved.
 */

#include "postgres.h"
#include "qos.h"
#include "hooks_internal.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/value.h"
#include "optimizer/planner.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include <strings.h>

#ifdef __linux__
#include <sched.h>
#endif

/* Per-backend resource tracking */
static bool cpu_affinity_set = false;

/* Forward declarations */
static void qos_adjust_parallel_workers(Plan *plan, int max_workers);

/*
 * Planner hook wrapper - adjust parallel workers based on CPU limits
 * Called from main hooks.c planner hook
 */
PlannedStmt *
qos_planner_hook(Query *parse, const char *query_string, int cursorOptions, 
                 ParamListInfo boundParams, planner_hook_type prev_hook)
{
    PlannedStmt *result;
    QoSLimits limits;
    int new_max_workers = 0;
    ListCell *lc;
    
    /* Call previous planner hook or standard planner first */
    if (prev_hook)
        result = prev_hook(parse, query_string, cursorOptions, boundParams);
    else
        result = standard_planner(parse, query_string, cursorOptions, boundParams);
    
    /* Apply QoS CPU limits to the planned statement */
    if (qos_enabled && result != NULL)
    {
        limits = qos_get_cached_limits();
        
        if (limits.cpu_core_limit > 0)
        {
            /* Calculate max allowed workers (cpu_core_limit - 1 for main process) */
            new_max_workers = (limits.cpu_core_limit > 1) ? (limits.cpu_core_limit - 1) : 0;
            
            /* Limit parallel workers in the main plan */
            if (result->parallelModeNeeded && result->planTree != NULL)
            {
                qos_adjust_parallel_workers(result->planTree, new_max_workers);
                
                elog(DEBUG2, "qos: adjusted parallel workers in plan (max: %d, cpu_core_limit=%d)",
                     new_max_workers, limits.cpu_core_limit);
            }
            
            /* Also adjust subplans */
            foreach(lc, result->subplans)
            {
                Plan *subplan = (Plan *) lfirst(lc);
                if (subplan != NULL)
                    qos_adjust_parallel_workers(subplan, new_max_workers);
            }
        }
    }
    
    return result;
}

/*
 * Recursively adjust parallel worker count in plan tree
 */
static void
qos_adjust_parallel_workers(Plan *plan, int max_workers)
{
    if (plan == NULL)
        return;
    
    /* Adjust workers for Gather nodes */
    if (IsA(plan, Gather))
    {
        Gather *gather = (Gather *) plan;
        if (gather->num_workers > max_workers)
        {
            elog(DEBUG3, "qos: limiting Gather workers from %d to %d",
                 gather->num_workers, max_workers);
            gather->num_workers = max_workers;
        }
    }
    /* Adjust workers for Gather Merge nodes */
    else if (IsA(plan, GatherMerge))
    {
        GatherMerge *gather_merge = (GatherMerge *) plan;
        if (gather_merge->num_workers > max_workers)
        {
            elog(DEBUG3, "qos: limiting GatherMerge workers from %d to %d",
                 gather_merge->num_workers, max_workers);
            gather_merge->num_workers = max_workers;
        }
    }
    
    /* Recursively process child plans */
    qos_adjust_parallel_workers(plan->lefttree, max_workers);
    qos_adjust_parallel_workers(plan->righttree, max_workers);
}

/*
 * Check and enforce CPU resource limits for current session
 * 
 * CPU Affinity (Linux only): Restricts total CPU usage to specific cores
 * Note: Parallel worker limiting is handled by planner_hook in hooks.c
 */
void
qos_enforce_cpu_limit(void)
{
    QoSLimits limits;
    
    if (!qos_enabled)
        return;
    
    /* Get cached limits - no catalog access needed */
    limits = qos_get_cached_limits();
    
    if (limits.cpu_core_limit <= 0)
        return;
    
    /* 
     * CPU Affinity - Total CPU usage restriction (Linux only)
     * Binds the entire backend process to specific CPU cores
     */
    if (!cpu_affinity_set)
    {
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
            elog(DEBUG1, "qos: CPU affinity set - limited to %d cores for user %u",
                 limits.cpu_core_limit, GetUserId());
        }
        else
        {
            elog(WARNING, "qos: failed to set CPU affinity for user %u: %m",
                 GetUserId());
        }
#else
        /* On non-Linux platforms, only parallel worker limiting is available (via planner hook) */
        elog(DEBUG2, "qos: CPU affinity not supported on this platform, parallel workers limited via planner");
        cpu_affinity_set = true; /* Mark as done to avoid repeated warnings */
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
    A_Const *arg;
    char *value_str = NULL;
    int64 new_work_mem_bytes;
    
    if (!qos_enabled)
        return;
    
    /* Only process work_mem SET commands */
    if (!stmt || !stmt->name || strcmp(stmt->name, "work_mem") != 0 || stmt->kind != VAR_SET_VALUE)
        return;
    
    /* Get cached limits - no catalog access needed */
    limits = qos_get_cached_limits();
    
    /* Check if limit is enabled */
    if (limits.work_mem_limit < 0 || stmt->args == NIL)
        return;
    
    /* Parse the new value */
    arg = (A_Const *) linitial(stmt->args);
    
    if (IsA(arg, A_Const))
    {
        if (nodeTag(&arg->val) == T_Integer)
        {
            /* Integer value in KB */
            new_work_mem_bytes = (int64)intVal(&arg->val) * 1024L;
        }
        else if (nodeTag(&arg->val) == T_String)
        {
            /* String value like "128MB" - use shared parser */
            value_str = strVal(&arg->val);
            new_work_mem_bytes = qos_parse_memory_unit(value_str);
        }
        else
        {
            return; /* Unknown value type */
        }
        
        /* Check limit */
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
                     errdetail("Requested %ld KB, maximum allowed is %ld KB",
                              new_work_mem_bytes / 1024, limits.work_mem_limit / 1024),
                     errhint("Contact administrator to increase qos.work_mem_limit")));
        }
    }
}
