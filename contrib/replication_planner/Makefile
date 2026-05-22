# =============================================================
# Makefile — replication_planner PostgreSQL Extension
# =============================================================

EXTENSION   = replication_planner
MODULE_big  = replication_planner
OBJS        = replication_planner.o

# SQL data files
DATA        = replication_planner--1.0.sql

# pg_config must be in PATH  (e.g. export PATH=/usr/lib/postgresql/16/bin:$PATH)
PG_CONFIG  ?= pg_config
PGXS       := $(shell $(PG_CONFIG) --pgxs)

# Extra CFLAGS for stricter builds
PG_CPPFLAGS = -Wall -Wextra -Wno-unused-parameter

include $(PGXS)

# ---- Convenience targets ----

.PHONY: install-dev reload help

## install-dev: build, install, and reload the extension in the running cluster
install-dev: install
	psql -c "SELECT pg_reload_conf();"

## reload: reload pg config (useful after GUC changes)
reload:
	psql -c "SELECT pg_reload_conf();"

## help: show this help
help:
	@echo "Targets:"
	@echo "  all          Build the shared library"
	@echo "  install      Install to \$$(pg_config --pkglibdir) and extension dirs"
	@echo "  install-dev  Build + install + reload cluster"
	@echo "  clean        Remove build artifacts"
	@echo "  installcheck Run regression tests"
