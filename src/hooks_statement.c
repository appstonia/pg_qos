/*
 * hooks_statement.c - Statement-level QoS tracking and enforcement
 *
 * This file implements concurrent statement tracking for SELECT, UPDATE,
 * DELETE, and INSERT operations.
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
#include "storage/lwlock.h"
#include "nodes/nodes.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/backendid.h"

/* Per-backend statement tracking */
static CmdType current_statement_type = CMD_UNKNOWN;
static bool statement_tracked = false;

/*
 * Track statement start - for SELECT, UPDATE, DELETE, INSERT concurrency limits
 */
void
qos_track_statement_start(CmdType operation)
{
    QoSLimits limits;
    int count = 0;
    int i;
    int limit_val = -1;
    
    if (!qos_enabled || statement_tracked)
        return;    
    
    limits = qos_get_cached_limits();
    
    /* Determine which limit applies */
    switch (operation)
    {
        case CMD_SELECT: limit_val = limits.max_concurrent_select; break;
        case CMD_UPDATE: limit_val = limits.max_concurrent_update; break;
        case CMD_DELETE: limit_val = limits.max_concurrent_delete; break;
        case CMD_INSERT: limit_val = limits.max_concurrent_insert; break;
        default: return;
    }
    
    if (qos_shared_state)
    {
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        
        /* Scan active backends to count current usage */
        for (i = 0; i < qos_shared_state->max_backends; i++)
        {
            /* Skip empty slots */
            if (qos_shared_state->backend_status[i].pid == 0)
                continue;
                
            /* Skip myself */
            if (i == MyBackendId - 1)
                continue;
            
            /* Count if matches my role, db, and operation */
            if (qos_shared_state->backend_status[i].role_oid == GetUserId() &&
                qos_shared_state->backend_status[i].database_oid == MyDatabaseId &&
                qos_shared_state->backend_status[i].cmd_type == operation)
            {
                count++;
            }
        }
        
        /* Check limit */
        if (limit_val > 0 && count >= limit_val)
        {
            /* Update stats */
            switch (operation)
            {
                case CMD_SELECT: qos_shared_state->stats.concurrent_select_violations++; break;
                case CMD_UPDATE: qos_shared_state->stats.concurrent_update_violations++; break;
                case CMD_DELETE: qos_shared_state->stats.concurrent_delete_violations++; break;
                case CMD_INSERT: qos_shared_state->stats.concurrent_insert_violations++; break;
                default: break;
            }
            qos_shared_state->stats.rejected_queries++;
            
            LWLockRelease(qos_shared_state->lock);
            
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("qos: maximum concurrent %s statements exceeded", 
                            operation == CMD_SELECT ? "SELECT" :
                            operation == CMD_UPDATE ? "UPDATE" :
                            operation == CMD_DELETE ? "DELETE" : "INSERT"),
                     errdetail("Current: %d, Maximum: %d", count, limit_val),
                     errhint("Wait for other queries to complete")));
        }
        
        /* Register myself */
        qos_shared_state->backend_status[MyBackendId - 1].pid = MyProcPid;
        qos_shared_state->backend_status[MyBackendId - 1].role_oid = GetUserId();
        qos_shared_state->backend_status[MyBackendId - 1].database_oid = MyDatabaseId;
        qos_shared_state->backend_status[MyBackendId - 1].cmd_type = operation;
        /* Preserve in_transaction state */
        
        LWLockRelease(qos_shared_state->lock);
        
        /* Only set tracking flags after successful registration */
        current_statement_type = operation;
        statement_tracked = true;
    }
}

/*
 * Track statement end - decrement statement-specific counters
 */
void
qos_track_statement_end(void)
{
    if (!qos_enabled || !statement_tracked)
        return;
    
    if (qos_shared_state)
    {
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        
        /* Clear my command type */
        if (qos_shared_state->backend_status[MyBackendId - 1].pid == MyProcPid)
        {
            qos_shared_state->backend_status[MyBackendId - 1].cmd_type = CMD_UNKNOWN;
        }
        
        LWLockRelease(qos_shared_state->lock);
    }
    
    statement_tracked = false;
    current_statement_type = CMD_UNKNOWN;
}
