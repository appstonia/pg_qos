/*
 * hooks_cache.c - QoS Cache Management
 *
 * This file implements the caching mechanism for QoS limits,
 * including invalidation callbacks and cache refresh logic.
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
#include "utils/inval.h"
#include "utils/syscache.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"

/* Cached QoS limits - invalidated via syscache callback */
static QoSLimits cached_limits = {-1, -1, -1, -1, -1, -1, -1};
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
 * Initialize cache system and register callbacks
 */
void
qos_init_cache(void)
{
    /* Register syscache invalidation callbacks for role and database changes */
    CacheRegisterSyscacheCallback(DATABASEOID, qos_invalidate_cache_callback, (Datum) 0);
    CacheRegisterSyscacheCallback(AUTHOID, qos_invalidate_cache_callback, (Datum) 0);
}

/*
 * Invalidate cache (public interface)
 */
void
qos_invalidate_cache(void)
{
    limits_cached = false;
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
    #define CALC_LIMIT(field) \
        if (role_limits.field >= 0 && db_limits.field >= 0) \
            cached_limits.field = Min(role_limits.field, db_limits.field); \
        else if (role_limits.field >= 0) \
            cached_limits.field = role_limits.field; \
        else if (db_limits.field >= 0) \
            cached_limits.field = db_limits.field; \
        else \
            cached_limits.field = -1;
    
    CALC_LIMIT(work_mem_limit);
    CALC_LIMIT(cpu_core_limit);
    CALC_LIMIT(max_concurrent_tx);
    CALC_LIMIT(max_concurrent_select);
    CALC_LIMIT(max_concurrent_update);
    CALC_LIMIT(max_concurrent_delete);
    CALC_LIMIT(max_concurrent_insert);
    
    #undef CALC_LIMIT

    /* Update cache metadata */
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
QoSLimits
qos_get_cached_limits(void)
{
    qos_refresh_cached_limits();
    return cached_limits;
}
