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
#include <unistd.h>

#ifdef __linux__
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <linux/perf_event.h>
#include <sys/syscall.h>
#include <sys/ioctl.h>
#include <errno.h>
#endif

/* Forward declarations */
static void qos_adjust_parallel_workers(Plan *plan, int max_workers);
#ifdef __linux__
static int qos_select_least_busy_cores(int *selected_cores, int requested_cores, int total_cores);
static long qos_measure_cpu_cycles(int cpu);
static int qos_get_or_assign_cores(Oid database_oid, Oid role_oid, int requested_cores, 
                                     int total_cores, int *assigned_cores);
#endif

/* Per-backend tracking: has work_mem been enforced yet? */
static bool work_mem_enforced = false;
static int work_mem_last_epoch = -1;

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
            elog(DEBUG3, "qos: limiting Gather Merge workers from %d to %d",
                 gather_merge->num_workers, max_workers);
            gather_merge->num_workers = max_workers;
        }
    }
    
    /* Recursively process child plans */
    qos_adjust_parallel_workers(plan->lefttree, max_workers);
    qos_adjust_parallel_workers(plan->righttree, max_workers);
}

#ifdef __linux__
/*
 * Measure CPU cycles for a specific CPU core using perf_event_open
 * Returns the number of cycles (lower = less busy), or -1 on error
 */
static long
qos_measure_cpu_cycles(int cpu)
{
    struct perf_event_attr pe;
    int fd;
    long long count;
    ssize_t bytes_read;
    
    memset(&pe, 0, sizeof(struct perf_event_attr));
    pe.type = PERF_TYPE_HARDWARE;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = PERF_COUNT_HW_CPU_CYCLES;
    pe.disabled = 1;
    pe.exclude_kernel = 0;
    pe.exclude_hv = 1;
    
    /* Open perf event for specific CPU */
    fd = syscall(__NR_perf_event_open, &pe, -1, cpu, -1, 0);
    if (fd == -1)
    {
        /* Permission denied or not supported - fallback to random */
        return -1;
    }
    
    /* Enable and read cycles */
    ioctl(fd, PERF_EVENT_IOC_RESET, 0);
    ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
    usleep(1000); /* Sample for 1ms */
    ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
    
    bytes_read = read(fd, &count, sizeof(long long));
    close(fd);
    
    if (bytes_read != sizeof(long long))
        return -1;
    
    return (long) count;
}

/*
 * Select the N least busy CPU cores based on real-time perf measurements
 * Returns the number of cores selected (writes core IDs to selected_cores array)
 */
static int
qos_select_least_busy_cores(int *selected_cores, int requested_cores, int total_cores)
{
    long *cpu_cycles;
    int *sorted_indices;
    int i, j, temp_idx;
    int valid_count = 0;
    
    if (requested_cores <= 0 || total_cores <= 0)
        return 0;
    
    if (requested_cores > total_cores)
        requested_cores = total_cores;
    
    cpu_cycles = (long *) palloc(sizeof(long) * total_cores);
    sorted_indices = (int *) palloc(sizeof(int) * total_cores);
    
    /* Measure cycles for each CPU */
    for (i = 0; i < total_cores; i++)
    {
        cpu_cycles[i] = qos_measure_cpu_cycles(i);
        sorted_indices[i] = i;
        
        if (cpu_cycles[i] >= 0)
            valid_count++;
    }
    
    /* If perf measurements failed, fallback to simple round-robin */
    if (valid_count == 0)
    {
        int start_core;
        
        /* Use shared memory counter for true round-robin across all backends */
        if (qos_shared_state)
        {
            LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
            start_core = qos_shared_state->next_cpu_core;
            qos_shared_state->next_cpu_core = (start_core + requested_cores) % total_cores;
            LWLockRelease(qos_shared_state->lock);
            
            elog(DEBUG1, "qos: perf unavailable, using round-robin - assigned cores starting at %d (pid=%d)",
                 start_core, (int)getpid());
        }
        else
        {
            /* Fallback if shared state not available */
            start_core = 0;
            elog(DEBUG1, "qos: no shared state, defaulting to core 0");
        }
        
        for (i = 0; i < requested_cores; i++)
        {
            selected_cores[i] = (start_core + i) % total_cores;
        }
        
        pfree(cpu_cycles);
        pfree(sorted_indices);
        return requested_cores;
    }
    
    /* Sort CPUs by cycle count (ascending = less busy first) */
    for (i = 0; i < requested_cores; i++)
    {
        int min_idx = i;
        for (j = i + 1; j < total_cores; j++)
        {
            /* Skip CPUs where measurement failed */
            if (cpu_cycles[sorted_indices[j]] < 0)
                continue;
            if (cpu_cycles[sorted_indices[min_idx]] < 0 || 
                cpu_cycles[sorted_indices[j]] < cpu_cycles[sorted_indices[min_idx]])
            {
                min_idx = j;
            }
        }
        if (min_idx != i)
        {
            temp_idx = sorted_indices[i];
            sorted_indices[i] = sorted_indices[min_idx];
            sorted_indices[min_idx] = temp_idx;
        }
    }
    
    /* Select the least busy N cores */
    for (i = 0; i < requested_cores; i++)
    {
        selected_cores[i] = sorted_indices[i];
    }
    
    elog(DEBUG1, "qos: selected %d cores using perf measurements (pid=%d)", 
         requested_cores, (int)getpid());
    
    pfree(cpu_cycles);
    pfree(sorted_indices);
    
    return requested_cores;
}

/*
 * Get or assign CPU cores for this db+role combination
 * Returns the number of cores assigned (writes core IDs to assigned_cores array)
 * 
 * If entry exists: Returns existing assigned cores
 * If entry doesn't exist: Selects new cores and stores them
 * 
 * Thread-safe: Uses LWLock to protect shared affinity_entries array
 */
static int
qos_get_or_assign_cores(Oid database_oid, Oid role_oid, int requested_cores, 
                        int total_cores, int *assigned_cores)
{
    int i, j;
    int empty_slot = -1;
    int num_cores = 0;
    
    if (!qos_shared_state || requested_cores <= 0)
        return 0;
    
    LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
    
    /* Search for existing entry */
    for (i = 0; i < MAX_AFFINITY_ENTRIES; i++)
    {
        if (qos_shared_state->affinity_entries[i].database_oid == database_oid &&
            qos_shared_state->affinity_entries[i].role_oid == role_oid)
        {
            /* Found existing entry - return assigned cores */
            num_cores = qos_shared_state->affinity_entries[i].num_cores;
            for (j = 0; j < num_cores && j < requested_cores; j++)
            {
                assigned_cores[j] = qos_shared_state->affinity_entries[i].assigned_cores[j];
            }
            LWLockRelease(qos_shared_state->lock);
            elog(DEBUG2, "qos: reusing existing core assignment for db=%u role=%u: %d cores (pid=%d)",
                 database_oid, role_oid, num_cores, (int)getpid());
            return num_cores;
        }
        
        /* Track first empty slot for insertion */
        if (empty_slot == -1 && 
            qos_shared_state->affinity_entries[i].database_oid == InvalidOid)
        {
            empty_slot = i;
        }
    }
    
    /* Not found - need to select new cores */
    LWLockRelease(qos_shared_state->lock);
    
    /* Select cores (this may take time with perf measurements) */
    num_cores = qos_select_least_busy_cores(assigned_cores, requested_cores, total_cores);
    
    if (num_cores <= 0)
        return 0;
    
    /* Store the assignment in shared memory */
    LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
    
    /* Re-check if another backend added this entry while we were selecting cores */
    for (i = 0; i < MAX_AFFINITY_ENTRIES; i++)
    {
        if (qos_shared_state->affinity_entries[i].database_oid == database_oid &&
            qos_shared_state->affinity_entries[i].role_oid == role_oid)
        {
            /* Another backend already added it - use their assignment */
            num_cores = qos_shared_state->affinity_entries[i].num_cores;
            for (j = 0; j < num_cores && j < requested_cores; j++)
            {
                assigned_cores[j] = qos_shared_state->affinity_entries[i].assigned_cores[j];
            }
            LWLockRelease(qos_shared_state->lock);
            elog(DEBUG1, "qos: another backend assigned cores for db=%u role=%u, using theirs (pid=%d)",
                 database_oid, role_oid, (int)getpid());
            return num_cores;
        }
    }
    
    /* Add new entry if space available */
    if (empty_slot != -1)
    {
        qos_shared_state->affinity_entries[empty_slot].database_oid = database_oid;
        qos_shared_state->affinity_entries[empty_slot].role_oid = role_oid;
        qos_shared_state->affinity_entries[empty_slot].num_cores = num_cores;
        for (j = 0; j < num_cores && j < MAX_CORES_PER_ENTRY; j++)
        {
            qos_shared_state->affinity_entries[empty_slot].assigned_cores[j] = assigned_cores[j];
        }
        LWLockRelease(qos_shared_state->lock);
        elog(DEBUG1, "qos: new core assignment for db=%u role=%u: %d cores (pid=%d)",
             database_oid, role_oid, num_cores, (int)getpid());
        return num_cores;
    }
    
    /* Array full - use LRU eviction (evict first entry, shift left) */
    for (i = 0; i < MAX_AFFINITY_ENTRIES - 1; i++)
    {
        qos_shared_state->affinity_entries[i] = qos_shared_state->affinity_entries[i + 1];
    }
    /* Add new entry at end */
    qos_shared_state->affinity_entries[MAX_AFFINITY_ENTRIES - 1].database_oid = database_oid;
    qos_shared_state->affinity_entries[MAX_AFFINITY_ENTRIES - 1].role_oid = role_oid;
    qos_shared_state->affinity_entries[MAX_AFFINITY_ENTRIES - 1].num_cores = num_cores;
    for (j = 0; j < num_cores && j < MAX_CORES_PER_ENTRY; j++)
    {
        qos_shared_state->affinity_entries[MAX_AFFINITY_ENTRIES - 1].assigned_cores[j] = assigned_cores[j];
    }
    
    LWLockRelease(qos_shared_state->lock);
    elog(DEBUG1, "qos: new core assignment (evicted LRU) for db=%u role=%u: %d cores (pid=%d)",
         database_oid, role_oid, num_cores, (int)getpid());
    return num_cores;
}
#endif

/*
 * Check and enforce CPU resource limits for current session
 * 
 * CPU Affinity (Linux only): Restricts total CPU usage to specific cores
 * Dynamically selects least-busy cores using perf_event_open measurements
 * Re-evaluated each time to ensure fair distribution across all cores
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
     * Gets or assigns cores for this db+role combination
     * Each backend for the same db+role will use the same cores
     */
#ifdef __linux__
    {
        cpu_set_t cpuset;
        long total_cpus;
        int requested_cores;
        int *assigned_cores;
        int num_assigned;
        int i;
        
        /* Get total available CPUs */
        total_cpus = sysconf(_SC_NPROCESSORS_ONLN);
        if (total_cpus <= 0)
            total_cpus = 1;
        
        /* Clamp requested cores to available CPUs */
        requested_cores = limits.cpu_core_limit;
        if (requested_cores > total_cpus)
        {
            elog(WARNING, "qos: cpu_core_limit=%d exceeds available CPUs=%ld, clamping to %ld",
                 requested_cores, total_cpus, total_cpus);
            requested_cores = (int) total_cpus;
        }
        
        assigned_cores = (int *) palloc(sizeof(int) * requested_cores);
        
        /* Get or assign cores for this db+role combination */
        num_assigned = qos_get_or_assign_cores(MyDatabaseId, GetUserId(), 
                                                requested_cores, (int) total_cpus, 
                                                assigned_cores);
        
        if (num_assigned > 0)
        {
            CPU_ZERO(&cpuset);
            for (i = 0; i < num_assigned; i++)
            {
                CPU_SET(assigned_cores[i], &cpuset);
            }
            
            if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) == 0)
            {
                elog(DEBUG3, "qos: CPU affinity set for db=%u role=%u pid=%d - using %d core(s): core %d%s",
                     MyDatabaseId, GetUserId(), (int)getpid(), num_assigned,
                     assigned_cores[0], num_assigned > 1 ? " (+ others)" : "");
            }
            else
            {
                elog(WARNING, "qos: failed to set CPU affinity for db=%u role=%u pid=%d: %m",
                     MyDatabaseId, GetUserId(), (int)getpid());
            }
        }
        
        pfree(assigned_cores);
    }
#else
    /* On non-Linux platforms, only parallel worker limiting is available (via planner hook) */
    elog(DEBUG5, "qos: CPU affinity not supported on this platform, parallel workers limited via planner");
#endif
}

/*
 * Enforce work_mem limit
 * 
 * 1. When user executes SET work_mem - validates the new value
 * 2. When session starts - enforces limit on current work_mem value
 */
void
qos_enforce_work_mem_limit(VariableSetStmt *stmt)
{
    QoSLimits limits;
    int64 new_work_mem_bytes;
    int current_work_mem_kb;
    int64 current_work_mem_bytes;
    
    if (!qos_enabled)
        return;
    
    /* Get cached limits - no catalog access needed */
    limits = qos_get_cached_limits();
    
    /* Check if limit is enabled */
    if (limits.work_mem_limit < 0)
        return;
    
    /* User is setting work_mem explicitly */
    if (stmt && stmt->name && strcmp(stmt->name, "work_mem") == 0 && 
        stmt->kind == VAR_SET_VALUE && stmt->args != NIL)
    {
        A_Const *arg = (A_Const *) linitial(stmt->args);
        
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
                char *value_str = strVal(&arg->val);
                new_work_mem_bytes = qos_parse_memory_unit(value_str);
            }
            else
            {
                return; /* Unknown value type */
            }
            
            /* Check if new value exceeds limit */
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
    /* Check and enforce limit on current work_mem value (called at session start) */
    else if (stmt == NULL)
    {
        int current_epoch;
        
        /* Check if epoch changed - if so, re-enforce work_mem */
        if (qos_shared_state)
        {
            current_epoch = qos_shared_state->settings_epoch;
            if (current_epoch != work_mem_last_epoch)
            {
                work_mem_enforced = false;
                work_mem_last_epoch = current_epoch;
                elog(DEBUG1, "qos: epoch changed %d -> %d, will re-enforce work_mem", 
                     work_mem_last_epoch == -1 ? -1 : work_mem_last_epoch, current_epoch);
            }
        }
        
        /* Only enforce once per backend per epoch to avoid repeated reductions */
        if (work_mem_enforced)
        {
            elog(DEBUG3, "qos: work_mem already enforced in this epoch, skipping");
            return;
        }
        
        work_mem_enforced = true;
        
        /* Get current work_mem setting (in KB) */
        current_work_mem_kb = work_mem;
        current_work_mem_bytes = (int64)current_work_mem_kb * 1024L;
        
        /* If current work_mem exceeds limit, cap it */
        if (current_work_mem_bytes > limits.work_mem_limit)
        {
            int new_work_mem_kb = (int)(limits.work_mem_limit / 1024L);
            
            /* Directly modify the global work_mem variable */
            work_mem = new_work_mem_kb;
            
            elog(LOG, "qos: work_mem enforced at %d KB (was %d KB) for db=%u role=%u",
                 new_work_mem_kb, current_work_mem_kb, MyDatabaseId, GetUserId());
            
            if (qos_shared_state)
            {
                LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
                qos_shared_state->stats.work_mem_violations++;
                LWLockRelease(qos_shared_state->lock);
            }
        }
    }
}
