/*
 * qos.h - PostgreSQL Quality of Service (QoS) Extension Header
 *
 * This header file contains the main definitions, constants, and function
 * prototypes for the PostgreSQL QoS extension, providing a unified interface
 * for quality of service management in PostgreSQL databases.
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

#ifndef QOS_H
#define QOS_H

#include "postgres.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "nodes/parsenodes.h"

/* QoS Limits Structure */
typedef struct QoSLimits
{
    int64   work_mem_limit;        /* Max work_mem in bytes (-1 = no limit) */    
} QoSLimits;

/* QoS Statistics */
typedef struct QoSStats
{    
    uint64  work_mem_violations;
} QoSStats;

/* Shared State */
typedef struct QoSSharedState
{
    LWLock     *lock;
    QoSStats    stats;
} QoSSharedState;

/* Global variables */
extern QoSSharedState *qos_shared_state;
extern bool qos_enabled;

/* exported functions */
extern void _PG_init(void);
extern void _PG_fini(void);

/* helper C functions callable from SQL */
extern Datum qos_version(PG_FUNCTION_ARGS);
extern Datum qos_get_stats(PG_FUNCTION_ARGS);
extern Datum qos_reset_stats(PG_FUNCTION_ARGS);
extern void qos_register_hooks(void);
extern void qos_unregister_hooks(void);

/* Function declarations */
extern QoSLimits qos_get_role_limits(Oid roleId);
extern QoSLimits qos_get_database_limits(Oid dbId);
extern void qos_enforce_work_mem_limit(VariableSetStmt *stmt);

#endif /* QOS_H */