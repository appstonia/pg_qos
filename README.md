# PostgreSQL QoS Resource Governor

PostgreSQL extension that provides Quality of Service (QoS) style resource governance for sessions and queries.

- Enforce per-role and per-database limits via `ALTER ROLE/DATABASE SET qos.*`
- Limit work_mem per session
- Limit CPU usage with a two-layer approach:
  - Limit parallel workers planned/launched (planner hook)
  - Bind backend to N CPU cores (CPU affinity, Linux only)
- Track and cap concurrent transactions and statements (SELECT/UPDATE/DELETE/INSERT)
- Fast, reliable cache invalidation across sessions (no reconnect) using a shared epoch mechanism

## Requirements

- PostgreSQL 15 or newer (officially supported)
- Build toolchain and server headers (`pg_config` must be available)
- Linux (optional) for CPU affinity; on other platforms, only parallel worker limiting is applied

Debian/Ubuntu packages:

- Install the server development package that matches your PostgreSQL version:
  - `postgresql-server-dev-15` (for PostgreSQL 15)
  - `postgresql-server-dev-16` (for PostgreSQL 16)
  - `postgresql-server-dev-17` (for PostgreSQL 17)
  - `postgresql-server-dev-18` (for PostgreSQL 18)

Example (Ubuntu/Debian):

```bash
sudo apt update
# Choose the version you run (15/16/17/18)
sudo apt install postgresql-server-dev-18 build-essential
```

## Build and Install

1. Build

```bash
make
```

1. Install

```bash
sudo make install
```

Notes for Debian/Ubuntu:

- Ensure the correct `postgresql-server-dev-<version>` is installed so `pg_config` points to your intended server version.
- If you have multiple PostgreSQL versions installed, you can point the build to a specific one by exporting PG_CONFIG:

```bash
export PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config
make clean && make
sudo make install
```

1. Enable the extension (server restart required due to hooks and shared memory)

Edit `postgresql.conf`:

```conf
shared_preload_libraries = 'qos'
```

Restart PostgreSQL, then in each database where you want QoS active:

```sql
CREATE EXTENSION qos;
```

## Configuration: qos.* settings

Configure limits per role and/or per database using standard GUC storage in `pg_db_role_setting`:

- `qos.work_mem_limit` (bytes) — max effective work_mem per session, e.g. `64MB`, `1GB`
- `qos.cpu_core_limit` (integer) — max CPU cores
- `qos.max_concurrent_tx` (integer) — max concurrent transactions
- `qos.max_concurrent_select` (integer) — max concurrent SELECT statement
- `qos.max_concurrent_update` (integer) — max concurrent UPDATE statement
- `qos.max_concurrent_delete` (integer) — max concurrent DELETE statement
- `qos.max_concurrent_insert` (integer) — max concurrent INSERT statement

Examples:

```sql
-- Per-role limits
ALTER ROLE app_user SET qos.work_mem_limit = '32MB';
ALTER ROLE app_user SET qos.cpu_core_limit = '2';
ALTER ROLE app_user SET qos.max_concurrent_select = '100';

-- Per-database limits (for all roles)
ALTER DATABASE appdb SET qos.max_concurrent_tx = '200';
```

Effective limits are the most restrictive combination of role-level and database-level settings.

## How it works

- Work_mem enforcement
  - Intercepts `SET work_mem` and rejects values above `qos.work_mem_limit`.

- CPU limiting
  - Planner hook reduces `Gather/GatherMerge` `num_workers` to respect `qos.cpu_core_limit - 1` (one core for the main backend).
  - On Linux, the backend can also be bound to the first N CPU cores (optional CPU affinity) to restrict total CPU use.

- Concurrency limits
  - Executor hooks track active transactions and statements per command type; caps are enforced against configured maxima.

- Cache invalidation (no reconnect needed)
  - On `ALTER ROLE/ALTER DATABASE SET/RESET qos.*`, a shared `settings_epoch` is bumped.
  - Each session observes `settings_epoch` and refreshes its cached limits on the next statement automatically.

## Observability and Logging

Increase verbosity temporarily to trace QoS activity:

```sql
SET client_min_messages = 'debug1';
```

You’ll see messages when cache is refreshed, CPU workers are adjusted, or limits are enforced.

## Limitations

- CPU affinity is only available on Linux; other platforms only enforce parallel worker limits via the planner.
- Requires `shared_preload_libraries` and a server restart to activate.
- Official support targets PostgreSQL 15 and newer.

## Uninstall

```sql
DROP EXTENSION IF EXISTS qos;
```

Remove from `shared_preload_libraries` and restart the server.

## Development

- Build with PGXS (Makefile provided)
- Targets PG15–PG18 server APIs
- Modular codebase:
  - `hooks.c`: hook registration and coordination
  - `hooks_cache.c`: session cache + shared epoch invalidation
  - `hooks_resource.c`: CPU/memory enforcement + planner hook
  - `hooks_statement.c`: statement-level concurrency tracking
  - `hooks_transaction.c`: transaction-level concurrency tracking
  - `qos.c`/`qos.h`: shared memory, catalog reads, helpers

## License

See the `LICENSE` file in this repository.
