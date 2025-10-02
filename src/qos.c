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