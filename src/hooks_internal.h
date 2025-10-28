/*
 * hooks_internal.h - Internal definitions for QoS hook modules
 *
 * This header file contains internal function declarations and shared
 * definitions used across different hook modules.
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

#ifndef QOS_HOOKS_INTERNAL_H
#define QOS_HOOKS_INTERNAL_H

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/planner.h"
#include "qos.h"

/* Cache management */
extern QoSLimits qos_get_cached_limits(void);
extern void qos_invalidate_cache(void);
extern void qos_init_cache(void);

/* Statement tracking functions (hooks_statement.c) */
extern void qos_track_statement_start(CmdType operation);
extern void qos_track_statement_end(void);

/* Transaction tracking functions (hooks_transaction.c) */
extern void qos_track_transaction_start(void);
extern void qos_track_transaction_end(void);

/* Resource enforcement functions (hooks_resource.c) */
extern void qos_enforce_cpu_limit(void);
extern void qos_enforce_work_mem_limit(VariableSetStmt *stmt);
extern PlannedStmt *qos_planner_hook(Query *parse, const char *query_string, 
                                      int cursorOptions, ParamListInfo boundParams,
                                      planner_hook_type prev_hook);

#endif /* QOS_HOOKS_INTERNAL_H */
