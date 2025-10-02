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