#include "postgres.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "executor/executor.h"
#include "utils/guc.h"
#include <sys/resource.h>
#include "hooks.h"

/* forward declarations for previous hooks */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
char *qos_work_mem_limit = NULL;

/* --- check limits --- */
static void qos_check_limits(void)
{
    /* here role-based limit checks can be implemented */
    if (qos_work_mem_limit)
    {
        SetConfigOption("work_mem",
                        qos_work_mem_limit,
                        PGC_USERSET,
                        PGC_S_SESSION);
        elog(DEBUG1, "qos: applied work_mem limit = %s", qos_work_mem_limit);
    }
}

/* --- ProcessUtility hook --- */
static void
qos_ProcessUtility(PlannedStmt *pstmt,
                   const char *queryString,
                   bool readOnlyTree,
                   ProcessUtilityContext context,
                   ParamListInfo params,
                   QueryEnvironment *queryEnv,
                   DestReceiver *dest,
                   QueryCompletion *qc)
{
    if (pstmt && pstmt->utilityStmt && IsA(pstmt->utilityStmt, VariableSetStmt))
    {
        VariableSetStmt *vstmt = (VariableSetStmt *) pstmt->utilityStmt;

        if (vstmt->name && strcmp(vstmt->name, "work_mem") == 0)
            ereport(ERROR, (errmsg("qos: changing work_mem is restricted by QoS policy")));
    }

    qos_check_limits();

    if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
}

/* --- ExecutorStart hook --- */
static void qos_ExecutorStart(QueryDesc *queryDesc, int eflags)
{

    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

/* --- Register/unregister hooks --- */
void qos_register_hooks(void)
{
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = qos_ProcessUtility;

    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = qos_ExecutorStart;

    elog(INFO, "qos: hooks registered");
}

void qos_unregister_hooks(void)
{
    ProcessUtility_hook = prev_ProcessUtility;
    ExecutorStart_hook = prev_ExecutorStart;

    elog(INFO, "qos: hooks unregistered");
}