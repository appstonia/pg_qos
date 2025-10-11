/*
 * hooks.h - PostgreSQL Quality of Service (QoS) Extension Hook Definitions
 *
 * This header file contains the function declarations and definitions for
 * the PostgreSQL QoS extension's hook system, enabling query and execution
 * control mechanisms.
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

#ifndef QOS_HOOKS_H
#define QOS_HOOKS_H

#include "postgres.h"
#include "tcop/utility.h"
#include "nodes/parsenodes.h"

/* Hook registration & unregistration functions */
extern void qos_register_hooks(void);
extern void qos_unregister_hooks(void);
extern void qos_check_and_enforce_limits(void);
extern void qos_enforce_work_mem_limit(VariableSetStmt *stmt);
extern void qos_track_transaction_start(void);
extern void qos_track_transaction_end(void);

#endif /* QOS_HOOKS_H */