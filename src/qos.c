/*
 * qos.c - PostgreSQL Quality of Service (QoS) Extension Main Module
 *
 * This file contains the main implementation of the PostgreSQL QoS extension,
 * including initialization, configuration management, and the extension's
 * primary functions for quality of service control.
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
#include "fmgr.h"
#include "qos.h"
#include "hooks.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "utils/syscache.h"
#include "access/xact.h"
#include "utils/array.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include <strings.h>

PG_MODULE_MAGIC;

/* Global state for QoS tracking */
QoSSharedState *qos_shared_state = NULL;

/* GUC variables */
bool qos_enabled = true;

/* Hook save variables */
shmem_startup_hook_type prev_shmem_startup_hook = NULL;
shmem_request_hook_type prev_shmem_request_hook = NULL;

/* Forward declarations */
static void qos_shmem_request(void);
static void qos_shmem_startup(void);
static int64 parse_memory_unit(const char *str);
static void parse_role_configs(ArrayType *configs, QoSLimits *limits);

PG_FUNCTION_INFO_V1(qos_version);
PG_FUNCTION_INFO_V1(qos_get_stats);
PG_FUNCTION_INFO_V1(qos_reset_stats);

Datum
qos_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text("PostgreSQL QoS Resource Governor 1.0"));
}

Datum
qos_get_stats(PG_FUNCTION_ARGS)
{
    /* TODO: Return current QoS statistics */
    PG_RETURN_TEXT_P(cstring_to_text("not yet implemented"));
}

Datum
qos_reset_stats(PG_FUNCTION_ARGS)
{
    if (qos_shared_state)
    {
        LWLockAcquire(qos_shared_state->lock, LW_EXCLUSIVE);
        memset(&qos_shared_state->stats, 0, sizeof(QoSStats));
        LWLockRelease(qos_shared_state->lock);
    }
    PG_RETURN_VOID();
}

/*
 * Request shared memory space for QoS tracking
 */
static void
qos_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(MAXALIGN(sizeof(QoSSharedState)));
    RequestNamedLWLockTranche("qos", 1);
}

/*
 * Initialize shared memory
 */
static void
qos_shmem_startup(void)
{
    bool found;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    
    qos_shared_state = ShmemInitStruct("qos_shared_state",
                                        sizeof(QoSSharedState),
                                        &found);
    
    if (!found)
    {
        /* Initialize shared state */
        memset(qos_shared_state, 0, sizeof(QoSSharedState));
        qos_shared_state->lock = &(GetNamedLWLockTranche("qos")->lock);
    }
    
    LWLockRelease(AddinShmemInitLock);
}

/*
 * Parse configuration array and extract QoS limits
 */
static void
parse_role_configs(ArrayType *configs, QoSLimits *limits)
{
    int nelems;
    Datum *elems;
    bool *nulls;
    int i;
    
    if (!configs)
        return;
        
    nelems = ArrayGetNItems(ARR_NDIM(configs), ARR_DIMS(configs));
    deconstruct_array(configs, TEXTOID, -1, false, TYPALIGN_INT,
                    &elems, &nulls, &nelems);
    
    for (i = 0; i < nelems; i++)
    {
        char *config_str;
        char *name, *value;
        
        if (nulls[i])
            continue;
        
        config_str = TextDatumGetCString(elems[i]);
        
        /* Parse "name=value" format */
        name = config_str;
        value = strchr(config_str, '=');
        if (value)
        {
            *value = '\0';
            value++;
            
            if (strcmp(name, "qos.work_mem_limit") == 0)
                limits->work_mem_limit = parse_memory_unit(value);
            else if (strcmp(name, "qos.cpu_core_limit") == 0)
                limits->cpu_core_limit = atoi(value);
            else if (strcmp(name, "qos.max_concurrent_tx") == 0)
                limits->max_concurrent_tx = atoi(value);            
        }
        
        pfree(config_str);
    }
    
    pfree(elems);
    pfree(nulls);
}

/*
 * Get QoS limits for current role using pg_db_role_setting
 */
QoSLimits
qos_get_role_limits(Oid roleId)
{
    QoSLimits limits;
    Relation pg_db_role_setting_rel;
    ScanKeyData scankey[2];
    SysScanDesc scan;
    HeapTuple tuple;
    
    /* Set defaults */
    limits.work_mem_limit = -1;
    limits.cpu_core_limit = -1;
    limits.max_concurrent_tx = -1;    
    
    /* Open pg_db_role_setting catalog */
    pg_db_role_setting_rel = table_open(DbRoleSettingRelationId, AccessShareLock);
    
    /* Scan for this role's settings (setdatabase = 0 means all databases) */
    ScanKeyInit(&scankey[0],
                Anum_pg_db_role_setting_setdatabase,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&scankey[1],
                Anum_pg_db_role_setting_setrole,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(roleId));
    
    scan = systable_beginscan(pg_db_role_setting_rel, DbRoleSettingDatidRolidIndexId,
                              true, NULL, 2, scankey);
    
    tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple))
    {
        bool isnull;
        Datum configDatum;
        
        configDatum = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
                                  RelationGetDescr(pg_db_role_setting_rel), &isnull);
        
        if (!isnull)
        {
            ArrayType *configs = DatumGetArrayTypeP(configDatum);
            parse_role_configs(configs, &limits);
        }
    }
    
    systable_endscan(scan);
    table_close(pg_db_role_setting_rel, AccessShareLock);
    
    return limits;
}

/*
 * Get QoS limits for current database using pg_db_role_setting
 */
QoSLimits
qos_get_database_limits(Oid dbId)
{
    QoSLimits limits;
    Relation pg_db_role_setting_rel;
    ScanKeyData scankey[2];
    SysScanDesc scan;
    HeapTuple tuple;
    
    /* Set defaults */
    limits.work_mem_limit = -1;
    limits.cpu_core_limit = -1;
    limits.max_concurrent_tx = -1;    
    
    /* Open pg_db_role_setting catalog */
    pg_db_role_setting_rel = table_open(DbRoleSettingRelationId, AccessShareLock);
    
    /* Scan for this database's settings (setrole = 0 means all roles) */
    ScanKeyInit(&scankey[0],
                Anum_pg_db_role_setting_setdatabase,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(dbId));
    ScanKeyInit(&scankey[1],
                Anum_pg_db_role_setting_setrole,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(InvalidOid));
    
    scan = systable_beginscan(pg_db_role_setting_rel, DbRoleSettingDatidRolidIndexId,
                              true, NULL, 2, scankey);
    
    tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple))
    {
        bool isnull;
        Datum configDatum;
        
        configDatum = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
                                  RelationGetDescr(pg_db_role_setting_rel), &isnull);
        
        if (!isnull)
        {
            ArrayType *configs = DatumGetArrayTypeP(configDatum);
            parse_role_configs(configs, &limits);
        }
    }
    
    systable_endscan(scan);
    table_close(pg_db_role_setting_rel, AccessShareLock);
    
    return limits;
}

/*
 * Parse memory unit string (e.g., "64MB", "1GB")
 */
static int64
parse_memory_unit(const char *str)
{
    char *endptr;
    int64 value = strtol(str, &endptr, 10);
    
    if (*endptr != '\0')
    {
        if (strcasecmp(endptr, "kb") == 0 || strcasecmp(endptr, "k") == 0)
            value *= 1024L;
        else if (strcasecmp(endptr, "mb") == 0 || strcasecmp(endptr, "m") == 0)
            value *= 1024L * 1024L;
        else if (strcasecmp(endptr, "gb") == 0 || strcasecmp(endptr, "g") == 0)
            value *= 1024L * 1024L * 1024L;
    }
    
    return value;
}

/* Module load/unload */
void _PG_init(void)
{
    if (!process_shared_preload_libraries_in_progress)
    {
        ereport(ERROR,
                (errmsg("qos must be loaded via shared_preload_libraries")));
        return;
    }

    /* Define GUC variables */
    DefineCustomBoolVariable("qos.enabled",
                            "Enable QoS resource governor",
                            NULL,
                            &qos_enabled,
                            true,
                            PGC_SIGHUP,
                            0,
                            NULL, NULL, NULL);

    /* Register shmem hooks */
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = qos_shmem_request;
    
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = qos_shmem_startup;

    /* Register execution hooks */
    qos_register_hooks();

    elog(INFO, "PostgreSQL QoS Resource Governor loaded");
}

void _PG_fini(void)
{
    /* Unregister hooks */
    qos_unregister_hooks();
    
    elog(INFO, "PostgreSQL QoS Resource Governor unloaded");
}