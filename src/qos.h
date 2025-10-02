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

PG_MODULE_MAGIC;

/* GUC variables */
extern int qos_cpu_limit;
extern char *qos_work_mem_limit;

/* Function prototypes */
extern void qos_check_limits(void);
extern void qos_register_hooks(void);
extern void qos_unregister_hooks(void);

#endif /* QOS_H */