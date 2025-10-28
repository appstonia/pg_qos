MODULE_big = qos
OBJS = src/qos.o src/hooks.o src/hooks_cache.o src/hooks_statement.o src/hooks_transaction.o src/hooks_resource.o
EXTENSION = qos
DATA = qos--1.0.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)
