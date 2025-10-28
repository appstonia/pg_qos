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

/* Per-backend transaction tracking */
static bool transaction_tracked = false;

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
