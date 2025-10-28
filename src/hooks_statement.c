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
void
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
