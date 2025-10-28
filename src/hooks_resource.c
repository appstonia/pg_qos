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
#include "nodes/value.h"
#include "storage/lwlock.h"
#include <strings.h>

#ifdef __linux__
#include <sched.h>
#endif

/* Per-backend resource tracking */
static bool cpu_affinity_set = false;

/*
 * Check and enforce CPU resource limits for current session
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
