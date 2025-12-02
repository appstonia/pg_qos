/*
 * hooks_transaction.c - Transaction-level QoS tracking
 *
 * This file implements concurrent transaction tracking and enforcement.
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
#include "miscadmin.h"
#include "storage/proc.h"

/* Per-backend transaction tracking */
static bool transaction_tracked = false;

/*
 * Track transaction start
 */
void
qos_track_transaction_start(void)
{
    QoSLimits limits;
    int count = 0;
    int i;
#ifndef MyBackendId
    int my_slot = -1;
#endif
    
    if (!qos_enabled || transaction_tracked)
        return;
    
    /* Get cached limits - no catalog access needed */
    limits = qos_get_cached_limits();
    
    if (qos_shared_state && limits.max_concurrent_tx > 0)
    {
#ifndef MyBackendId
    my_slot = qos_get_backend_slot(true);
#endif
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        
        /* Scan active backends to count current usage */
        for (i = 0; i < qos_shared_state->max_backends; i++)
        {
            /* Skip empty slots */
            if (qos_shared_state->backend_status[i].pid == 0)
                continue;
                
            /* Skip myself */
                if (
#ifndef MyBackendId
                i == my_slot
#else
                i == MyBackendId - 1
#endif
                )
                continue;
            
            /* Count if matches my role, db, and is in transaction */
            if (qos_shared_state->backend_status[i].role_oid == GetUserId() &&
                qos_shared_state->backend_status[i].database_oid == MyDatabaseId &&
                qos_shared_state->backend_status[i].in_transaction)
            {
                count++;
            }
        }
        
        if (count >= limits.max_concurrent_tx)
        {
            qos_shared_state->stats.concurrent_tx_violations++;
            qos_shared_state->stats.rejected_queries++;
            LWLockRelease(qos_shared_state->lock);
            
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("qos: maximum concurrent transactions exceeded"),
                     errdetail("Current: %d, Maximum: %d", count, limits.max_concurrent_tx),
                     errhint("Wait for other transactions to complete")));
        }
        
        /* Register myself */
    #ifndef MyBackendId
        if (my_slot >= 0)
        {
            qos_shared_state->backend_status[my_slot].role_oid = GetUserId();
            qos_shared_state->backend_status[my_slot].database_oid = MyDatabaseId;
            qos_shared_state->backend_status[my_slot].in_transaction = true;
        }
    #else
        qos_shared_state->backend_status[MyBackendId - 1].pid = MyProcPid;
        qos_shared_state->backend_status[MyBackendId - 1].role_oid = GetUserId();
        qos_shared_state->backend_status[MyBackendId - 1].database_oid = MyDatabaseId;
        qos_shared_state->backend_status[MyBackendId - 1].in_transaction = true;
    #endif
        
        LWLockRelease(qos_shared_state->lock);
        
        /* Only set tracking flag after successful increment */
        transaction_tracked = true;
    }
}

/*
 * Track transaction end
 */
void
qos_track_transaction_end(void)
{
#ifndef MyBackendId
    int my_slot = -1;
#endif
    if (!qos_enabled || !transaction_tracked)
        return;
    
    if (qos_shared_state)
    {
#ifndef MyBackendId
    my_slot = qos_get_backend_slot(false);
#endif
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        
        /* Clear my transaction flag */
        if (
    #ifndef MyBackendId
            my_slot >= 0 && qos_shared_state->backend_status[my_slot].pid == MyProcPid
    #else
            qos_shared_state->backend_status[MyBackendId - 1].pid == MyProcPid
    #endif
        )
        {
    #ifndef MyBackendId
            qos_shared_state->backend_status[my_slot].in_transaction = false;
    #else
            qos_shared_state->backend_status[MyBackendId - 1].in_transaction = false;
    #endif
        }
        
        LWLockRelease(qos_shared_state->lock);
    }
    
    transaction_tracked = false;
}
