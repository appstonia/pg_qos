MODULE_big = qos
OBJS = src/qos.o src/hooks.o
EXTENSION = qos
DATA = qos--1.0.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)
