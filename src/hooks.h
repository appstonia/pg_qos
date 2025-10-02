#ifndef QOS_HOOKS_H
#define QOS_HOOKS_H

#include "postgres.h"
#include "tcop/utility.h"

/* Hook registration & unregistration functions */
extern void qos_register_hooks(void);
extern void qos_unregister_hooks(void);

#endif /* QOS_HOOKS_H */