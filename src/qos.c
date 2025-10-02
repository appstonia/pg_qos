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
#include "utils/builtins.h"
#include "qos.h"
#include "utils/guc.h"

/* QoS version information */
PG_FUNCTION_INFO_V1(qos_version);

Datum
qos_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text("qos 1.0 (unified hooks + limits)"));
}

/* Extension init & fini */
void _PG_init(void)
{
    DefineCustomStringVariable("qos.work_mem_limit",
                               "QoS memory limit per role",
                               NULL,
                               &qos_work_mem_limit,
                               NULL,
                               PGC_USERSET,
                               0,
                               NULL, NULL, NULL);
    
    qos_register_hooks();
    elog(LOG, "qos: module initialized");
}

void _PG_fini(void)
{
    qos_unregister_hooks();
    elog(LOG, "qos: cleaned up hooks");    
}