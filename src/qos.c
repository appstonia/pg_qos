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
#include "access/transam.h"
#include "utils/array.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "catalog/indexing.h"
#include <strings.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>

PG_MODULE_MAGIC;

/* Global state for QoS tracking */
QoSSharedState *qos_shared_state = NULL;

/* GUC variables */
bool qos_enabled = true;

/* Hook save variables */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

/* Forward declarations */
static void qos_shmem_request(void);
static void qos_shmem_startup(void);
static void parse_role_configs(ArrayType *configs, QoSLimits *limits);

static const char *qos_valid_param_hint =
    "Valid parameters: qos.work_mem_limit, qos.cpu_core_limit, "
    "qos.max_concurrent_tx, qos.max_concurrent_select, "
    "qos.max_concurrent_update, qos.max_concurrent_delete, "
    "qos.max_concurrent_insert, qos.work_mem_error_level";

static char *qos_trim_whitespace(char *str);
static bool qos_parse_int32_value(const char *value_str, int *out,
                                  int min_value, int max_value,
                                  bool allow_negative_one,
                                  const char *param_name, bool strict);
static bool qos_parse_memory_value(const char *value_str, int64 *out,
                                   const char *param_name, bool strict);
static bool qos_parse_work_mem_error_level(const char *value_str,
                                           const char *param_name, bool strict);
static bool qos_is_valid_qos_param_name_internal(const char *name);
static bool qos_is_valid_qos_setting_entry(const char *config_str);
static ArrayType *qos_cleanup_invalid_qos_settings(Relation rel, HeapTuple tuple,
                                                   ArrayType *configs);
static char *qos_normalize_work_mem_value(const char *value_str);

bool qos_is_valid_qos_param_name(const char *name);
bool qos_apply_qos_param_value(QoSLimits *limits, const char *name,
                               const char *value, bool strict);

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
    PG_RETURN_TEXT_P(cstring_to_text("not implemented yet"));
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
    Size size;

    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    /* Calculate size needed for shared state + per-backend status array */
    size = sizeof(QoSSharedState);
    size = add_size(size, mul_size(MaxBackends, sizeof(QoSBackendStatus)));
    
    RequestAddinShmemSpace(MAXALIGN(size));
    RequestNamedLWLockTranche("qos", 1);
}

/*
 * Initialize shared memory
 */
static void
qos_shmem_startup(void)
{
    bool found;
    Size size;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    /* Calculate size needed for shared state + per-backend status array */
    size = sizeof(QoSSharedState);
    size = add_size(size, mul_size(MaxBackends, sizeof(QoSBackendStatus)));

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    
    qos_shared_state = ShmemInitStruct("qos_shared_state",
                                        size,
                                        &found);
    
    if (!found)
    {
        int i;
        
        /* Initialize shared state */
        memset(qos_shared_state, 0, size);
        qos_shared_state->lock = &(GetNamedLWLockTranche("qos")->lock);
        qos_shared_state->settings_epoch = 0;
        qos_shared_state->next_cpu_core = 0;
        qos_shared_state->max_backends = MaxBackends;
        
        /* Initialize affinity tracking array */
        for (i = 0; i < MAX_AFFINITY_ENTRIES; i++)
        {
            qos_shared_state->affinity_entries[i].database_oid = InvalidOid;
            qos_shared_state->affinity_entries[i].role_oid = InvalidOid;
            qos_shared_state->affinity_entries[i].num_cores = 0;
        }
        
        /* Initialize backend status array */
        for (i = 0; i < MaxBackends; i++)
        {
            qos_shared_state->backend_status[i].pid = 0;
            qos_shared_state->backend_status[i].role_oid = InvalidOid;
            qos_shared_state->backend_status[i].database_oid = InvalidOid;
            qos_shared_state->backend_status[i].cmd_type = CMD_UNKNOWN;
            qos_shared_state->backend_status[i].in_transaction = false;
        }
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
        char *name;
        char *value;

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

            name = qos_trim_whitespace(name);
            value = qos_trim_whitespace(value);

            if (pg_strncasecmp(name, "qos.", 4) == 0)
                (void) qos_apply_qos_param_value(limits, name, value, false);
        }
        else if (pg_strncasecmp(config_str, "qos.", 4) == 0)
        {
            elog(DEBUG1, "qos: invalid parameter format \"%s\" (expected name=value)",
                 config_str);
        }

        pfree(config_str);
    }

    pfree(elems);
    pfree(nulls);
}

static char *
qos_trim_whitespace(char *str)
{
    char *end;

    if (str == NULL)
        return NULL;

    while (*str != '\0' && isspace((unsigned char) *str))
        str++;

    if (*str == '\0')
        return str;

    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char) *end))
    {
        *end = '\0';
        end--;
    }

    return str;
}

static bool
qos_parse_int32_value(const char *value_str, int *out,
                      int min_value, int max_value,
                      bool allow_negative_one,
                      const char *param_name, bool strict)
{
    char *endptr;
    long value;

    if (value_str == NULL || *value_str == '\0')
        goto invalid;

    errno = 0;
    value = strtol(value_str, &endptr, 10);
    if (endptr == value_str || *endptr != '\0' || errno == ERANGE)
        goto invalid;

    if (allow_negative_one && value == -1)
    {
        if (out)
            *out = -1;
        return true;
    }

    if (value < min_value || value > max_value)
        goto invalid;

    if (out)
        *out = (int) value;
    return true;

invalid:
    if (strict)
        ereport(ERROR,
                (errmsg("qos: invalid value for %s: \"%s\"", param_name, value_str)));
    else
        elog(DEBUG1, "qos: invalid value for %s: \"%s\" (ignored)", param_name, value_str);
    return false;
}

static bool
qos_parse_memory_value(const char *value_str, int64 *out,
                       const char *param_name, bool strict)
{
    char *endptr;
    long long base;
    int64 multiplier = 1;
    const char *suffix;

    if (value_str == NULL || *value_str == '\0')
        goto invalid;

    errno = 0;
    base = strtoll(value_str, &endptr, 10);
    if (endptr == value_str || errno == ERANGE)
        goto invalid;

    suffix = endptr;
    while (*suffix != '\0' && isspace((unsigned char) *suffix))
        suffix++;

    if (*suffix != '\0')
    {
        if (pg_strcasecmp(suffix, "kb") == 0 || pg_strcasecmp(suffix, "k") == 0)
            multiplier = 1024L;
        else if (pg_strcasecmp(suffix, "mb") == 0 || pg_strcasecmp(suffix, "m") == 0)
            multiplier = 1024L * 1024L;
        else if (pg_strcasecmp(suffix, "gb") == 0 || pg_strcasecmp(suffix, "g") == 0)
            multiplier = 1024L * 1024L * 1024L;
        else
            goto invalid;
    }

    if (base == -1 && *suffix != '\0')
        goto invalid;

    if (base < -1)
        goto invalid;

    if (base > 0 && multiplier > 1)
    {
        if ((int64) base > (INT64_MAX / multiplier))
            goto invalid;
    }

    if (out)
        *out = (int64) base * multiplier;
    return true;

invalid:
    if (strict)
        ereport(ERROR,
                (errmsg("qos: invalid value for %s: \"%s\"", param_name, value_str),
                 errdetail("Expected a number with optional unit (kB, MB, GB) or -1.")));
    else
        elog(DEBUG1, "qos: invalid value for %s: \"%s\" (ignored)", param_name, value_str);
    return false;
}

static bool
qos_parse_work_mem_error_level(const char *value_str,
                               const char *param_name, bool strict)
{
    if (value_str == NULL || *value_str == '\0')
        goto invalid;

    if (pg_strcasecmp(value_str, "warning") == 0 || pg_strcasecmp(value_str, "error") == 0)
        return true;

invalid:
    if (strict)
        ereport(ERROR,
                (errmsg("qos: invalid value for %s: \"%s\"", param_name, value_str),
                 errdetail("Expected \"warning\" or \"error\".")));
    else
        elog(DEBUG1, "qos: invalid value for %s: \"%s\" (ignored)", param_name, value_str);
    return false;
}

static bool
qos_is_valid_qos_param_name_internal(const char *name)
{
    if (name == NULL)
        return false;

    if (strcmp(name, "qos.work_mem_limit") == 0)
        return true;
    if (strcmp(name, "qos.cpu_core_limit") == 0)
        return true;
    if (strcmp(name, "qos.max_concurrent_tx") == 0)
        return true;
    if (strcmp(name, "qos.max_concurrent_select") == 0)
        return true;
    if (strcmp(name, "qos.max_concurrent_update") == 0)
        return true;
    if (strcmp(name, "qos.max_concurrent_delete") == 0)
        return true;
    if (strcmp(name, "qos.max_concurrent_insert") == 0)
        return true;
    if (strcmp(name, "qos.enabled") == 0)
        return true;
    if (strcmp(name, "qos.work_mem_error_level") == 0)
        return true;

    return false;
}

bool
qos_is_valid_qos_param_name(const char *name)
{
    return qos_is_valid_qos_param_name_internal(name);
}

bool
qos_apply_qos_param_value(QoSLimits *limits, const char *name,
                          const char *value, bool strict)
{
    int parsed_int = -1;
    int64 parsed_mem = -1;
    char *value_copy = NULL;
    char *trimmed_value = NULL;

    if (name == NULL)
        return false;

    if (pg_strncasecmp(name, "qos.", 4) != 0)
        return false;

    if (!qos_is_valid_qos_param_name_internal(name))
    {
        if (strict)
            ereport(ERROR,
                    (errmsg("qos: invalid parameter name \"%s\"", name),
                     errhint("%s", qos_valid_param_hint)));
        else
            elog(DEBUG1, "qos: invalid parameter name \"%s\" (ignored)", name);
        return false;
    }

    if (strcmp(name, "qos.enabled") == 0)
        return true;

    if (value == NULL)
    {
        if (strict)
            ereport(ERROR,
                    (errmsg("qos: missing value for parameter \"%s\"", name)));
        else
            elog(DEBUG1, "qos: missing value for parameter \"%s\" (ignored)", name);
        return false;
    }

    value_copy = pstrdup(value);
    trimmed_value = qos_trim_whitespace(value_copy);

    if (strcmp(name, "qos.work_mem_limit") == 0)
    {
        if (!qos_parse_memory_value(trimmed_value, &parsed_mem, name, strict))
        {
            pfree(value_copy);
            return false;
        }
        if (limits)
            limits->work_mem_limit = parsed_mem;
        pfree(value_copy);
        return true;
    }

    if (strcmp(name, "qos.cpu_core_limit") == 0)
    {
        if (!qos_parse_int32_value(trimmed_value, &parsed_int, 0, INT_MAX, true, name, strict))
        {
            pfree(value_copy);
            return false;
        }
        if (limits)
            limits->cpu_core_limit = parsed_int;
        pfree(value_copy);
        return true;
    }

    if (strcmp(name, "qos.max_concurrent_tx") == 0)
    {
        if (!qos_parse_int32_value(trimmed_value, &parsed_int, 0, INT_MAX, true, name, strict))
        {
            pfree(value_copy);
            return false;
        }
        if (limits)
            limits->max_concurrent_tx = parsed_int;
        pfree(value_copy);
        return true;
    }

    if (strcmp(name, "qos.max_concurrent_select") == 0)
    {
        if (!qos_parse_int32_value(trimmed_value, &parsed_int, 0, INT_MAX, true, name, strict))
        {
            pfree(value_copy);
            return false;
        }
        if (limits)
            limits->max_concurrent_select = parsed_int;
        pfree(value_copy);
        return true;
    }

    if (strcmp(name, "qos.max_concurrent_update") == 0)
    {
        if (!qos_parse_int32_value(trimmed_value, &parsed_int, 0, INT_MAX, true, name, strict))
        {
            pfree(value_copy);
            return false;
        }
        if (limits)
            limits->max_concurrent_update = parsed_int;
        pfree(value_copy);
        return true;
    }

    if (strcmp(name, "qos.max_concurrent_delete") == 0)
    {
        if (!qos_parse_int32_value(trimmed_value, &parsed_int, 0, INT_MAX, true, name, strict))
        {
            pfree(value_copy);
            return false;
        }
        if (limits)
            limits->max_concurrent_delete = parsed_int;
        pfree(value_copy);
        return true;
    }

    if (strcmp(name, "qos.max_concurrent_insert") == 0)
    {
        if (!qos_parse_int32_value(trimmed_value, &parsed_int, 0, INT_MAX, true, name, strict))
        {
            pfree(value_copy);
            return false;
        }
        if (limits)
            limits->max_concurrent_insert = parsed_int;
        pfree(value_copy);
        return true;
    }

    if (strcmp(name, "qos.work_mem_error_level") == 0)
    {
        if (!qos_parse_work_mem_error_level(trimmed_value, name, strict))
        {
            pfree(value_copy);
            return false;
        }
        if (limits)
            limits->work_mem_error_level = (pg_strcasecmp(trimmed_value, "error") == 0)
                                             ? QOS_WORK_MEM_ERROR_ERROR
                                             : QOS_WORK_MEM_ERROR_WARNING;
        pfree(value_copy);
        return true;
    }

    if (value_copy)
        pfree(value_copy);
    return false;
}

static bool
qos_is_valid_qos_setting_entry(const char *config_str)
{
    char *copy;
    char *name;
    char *value;
    bool valid = false;

    if (config_str == NULL)
        return false;

    copy = pstrdup(config_str);
    name = copy;
    value = strchr(copy, '=');
    if (!value)
    {
        pfree(copy);
        return false;
    }

    *value = '\0';
    value++;

    name = qos_trim_whitespace(name);
    value = qos_trim_whitespace(value);

    if (pg_strncasecmp(name, "qos.", 4) != 0)
    {
        pfree(copy);
        return true;
    }

    valid = qos_apply_qos_param_value(NULL, name, value, false);
    pfree(copy);
    return valid;
}

static char *
qos_normalize_work_mem_value(const char *value_str)
{
    const char *ptr;
    const char *num_start;
    const char *unit_start;
    size_t num_len;
    char *num_part;
    char *unit_part;
    const char *normalized_unit = NULL;

    if (value_str == NULL)
        return NULL;

    ptr = value_str;
    while (*ptr != '\0' && isspace((unsigned char) *ptr))
        ptr++;

    num_start = ptr;
    while (*ptr != '\0' && isdigit((unsigned char) *ptr))
        ptr++;

    if (ptr == num_start)
        return NULL;

    num_len = (size_t) (ptr - num_start);
    num_part = palloc(num_len + 1);
    memcpy(num_part, num_start, num_len);
    num_part[num_len] = '\0';

    while (*ptr != '\0' && isspace((unsigned char) *ptr))
        ptr++;

    unit_start = ptr;
    while (*ptr != '\0' && isalpha((unsigned char) *ptr))
        ptr++;

    if (*ptr != '\0')
    {
        pfree(num_part);
        return NULL;
    }

    if (unit_start == ptr)
    {
        normalized_unit = "MB";
    }
    else
    {
        unit_part = pnstrdup(unit_start, ptr - unit_start);
        if (pg_strcasecmp(unit_part, "k") == 0 || pg_strcasecmp(unit_part, "kb") == 0)
            normalized_unit = "kB";
        else if (pg_strcasecmp(unit_part, "m") == 0 || pg_strcasecmp(unit_part, "mb") == 0)
            normalized_unit = "MB";
        else if (pg_strcasecmp(unit_part, "g") == 0 || pg_strcasecmp(unit_part, "gb") == 0)
            normalized_unit = "GB";
        pfree(unit_part);
    }

    if (!normalized_unit)
    {
        pfree(num_part);
        return NULL;
    }

    {
        char *result = psprintf("%s%s", num_part, normalized_unit);
        pfree(num_part);
        return result;
    }
}

static ArrayType *
qos_cleanup_invalid_qos_settings(Relation rel, HeapTuple tuple, ArrayType *configs)
{
    int nelems;
    Datum *elems;
    bool *nulls;
    int i;
    int kept = 0;
    bool removed = false;
    Datum *new_elems;
    ArrayType *new_configs;
    static Oid last_cleaned_db = InvalidOid;
    static Oid last_cleaned_role = InvalidOid;
    static CommandId last_cleaned_cmdid = InvalidCommandId;
    Oid setdatabase;
    Oid setrole;
    CommandId cmdid;
    bool isnull;

    if (!configs)
        return configs;

    nelems = ArrayGetNItems(ARR_NDIM(configs), ARR_DIMS(configs));
    if (nelems <= 0)
        return configs;

    cmdid = GetCurrentCommandId(false);
    setdatabase = DatumGetObjectId(heap_getattr(tuple, Anum_pg_db_role_setting_setdatabase,
                                                RelationGetDescr(rel), &isnull));
    if (isnull)
        setdatabase = InvalidOid;
    setrole = DatumGetObjectId(heap_getattr(tuple, Anum_pg_db_role_setting_setrole,
                                            RelationGetDescr(rel), &isnull));
    if (isnull)
        setrole = InvalidOid;

    if (cmdid == last_cleaned_cmdid &&
        setdatabase == last_cleaned_db &&
        setrole == last_cleaned_role)
        return configs;

    deconstruct_array(configs, TEXTOID, -1, false, TYPALIGN_INT,
                      &elems, &nulls, &nelems);

    new_elems = (Datum *) palloc(sizeof(Datum) * nelems);

    for (i = 0; i < nelems; i++)
    {
        char *config_str;

        if (nulls[i])
        {
            removed = true;
            continue;
        }

        config_str = TextDatumGetCString(elems[i]);
        if (pg_strncasecmp(config_str, "qos.", 4) == 0)
        {
            char *copy;
            char *name;
            char *value;

            if (!qos_is_valid_qos_setting_entry(config_str))
            {
                removed = true;
                pfree(config_str);
                continue;
            }

            copy = pstrdup(config_str);
            name = copy;
            value = strchr(copy, '=');
            if (value)
            {
                *value = '\0';
                value++;

                name = qos_trim_whitespace(name);
                value = qos_trim_whitespace(value);

                if (strcmp(name, "qos.work_mem_limit") == 0)
                {
                    char *normalized_value = qos_normalize_work_mem_value(value);

                    if (normalized_value && strcmp(normalized_value, value) != 0)
                    {
                        char *normalized = psprintf("%s=%s", name, normalized_value);

                        new_elems[kept++] = CStringGetTextDatum(normalized);
                        removed = true;
                        pfree(normalized_value);
                        pfree(normalized);
                        pfree(copy);
                        pfree(config_str);
                        continue;
                    }

                    if (normalized_value)
                        pfree(normalized_value);
                }
            }

            pfree(copy);
        }

        new_elems[kept++] = CStringGetTextDatum(config_str);
        pfree(config_str);
    }

    if (!removed)
    {
        pfree(new_elems);
        pfree(elems);
        pfree(nulls);
        return configs;
    }

    if (kept > 0)
        new_configs = construct_array(new_elems, kept, TEXTOID, -1, false, TYPALIGN_INT);
    else
        new_configs = construct_empty_array(TEXTOID);

    {
        Datum values[Natts_pg_db_role_setting];
        bool nulls_replace[Natts_pg_db_role_setting];
        bool replaces[Natts_pg_db_role_setting];
        HeapTuple newtuple;
        int j;

        for (j = 0; j < Natts_pg_db_role_setting; j++)
        {
            values[j] = (Datum) 0;
            nulls_replace[j] = false;
            replaces[j] = false;
        }

        values[Anum_pg_db_role_setting_setconfig - 1] = PointerGetDatum(new_configs);
        replaces[Anum_pg_db_role_setting_setconfig - 1] = true;

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), values, nulls_replace, replaces);
        CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
        heap_freetuple(newtuple);
    }

    last_cleaned_db = setdatabase;
    last_cleaned_role = setrole;
    last_cleaned_cmdid = cmdid;

    pfree(new_elems);
    pfree(elems);
    pfree(nulls);

    return new_configs;
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
    limits.max_concurrent_select = -1;
    limits.max_concurrent_update = -1;
    limits.max_concurrent_delete = -1;
    limits.max_concurrent_insert = -1;
    limits.work_mem_error_level = -1;
    
    /* Open pg_db_role_setting catalog */
    pg_db_role_setting_rel = table_open(DbRoleSettingRelationId, RowExclusiveLock);
    
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
            ArrayType *cleaned_configs = qos_cleanup_invalid_qos_settings(pg_db_role_setting_rel,
                                                                         tuple, configs);
            parse_role_configs(cleaned_configs, &limits);
            
            /* Free detoasted copy if it was created */
            if ((Pointer) configs != DatumGetPointer(configDatum))
                pfree(configs);
            if (cleaned_configs != configs && (Pointer) cleaned_configs != DatumGetPointer(configDatum))
                pfree(cleaned_configs);
        }
    }
    
    systable_endscan(scan);
    table_close(pg_db_role_setting_rel, RowExclusiveLock);
    
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
    limits.max_concurrent_select = -1;
    limits.max_concurrent_update = -1;
    limits.max_concurrent_delete = -1;
    limits.max_concurrent_insert = -1;
    limits.work_mem_error_level = -1;
    
    /* Open pg_db_role_setting catalog */
    pg_db_role_setting_rel = table_open(DbRoleSettingRelationId, RowExclusiveLock);
    
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
            ArrayType *cleaned_configs = qos_cleanup_invalid_qos_settings(pg_db_role_setting_rel,
                                                                         tuple, configs);
            parse_role_configs(cleaned_configs, &limits);
            
            /* Free detoasted copy if it was created */
            if ((Pointer) configs != DatumGetPointer(configDatum))
                pfree(configs);
            if (cleaned_configs != configs && (Pointer) cleaned_configs != DatumGetPointer(configDatum))
                pfree(cleaned_configs);
        }
    }
    
    systable_endscan(scan);
    table_close(pg_db_role_setting_rel, RowExclusiveLock);
    
    return limits;
}

/*
 * Parse memory unit string (e.g., "64MB", "1GB")
 * Public function - can be used by other modules
 */
int64
qos_parse_memory_unit(const char *str)
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