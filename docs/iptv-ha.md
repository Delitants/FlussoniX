# IPTV authority high availability

This optional profile lets two FlussoniX IPTV authorities share a PostgreSQL catalog and lease ledger. Viewer nodes can configure both HTTPS authorities; a retry keeps the same operation ID, so an uncertain response cannot create another session. Standalone `iptv.json` installations continue to work as before. Session limits cover FlussoniX viewers only.

## Prepare the database

Use a dedicated PostgreSQL database with verified TLS. The credential JSON is an absolute, owner-only (0600) file containing `cluster_id`, `host`, `port`, `database`, `user`, `password`, and the absolute `ca_pem` path. Keep the database password out of command-line arguments, `config.json`, logs and backups that are not access-controlled.

Initialize the schema once with a database owner account:

```bash
flussonix --iptv-lease-authority --iptv-ha-db-config /absolute/private/owner-db.json --iptv-ha-init-empty
```

To import a validated standalone IPTV file into an empty schema, replace `--iptv-ha-init-empty` with `--iptv-ha-init-from /absolute/private/iptv.json`. Initialization exits without starting listeners and refuses an existing schema. Regular startup never imports the standalone file. Back up the database and the input file before migration. The owner-only catalog export is a recovery aid; PostgreSQL remains canonical.

Use a separate runtime account. After initialization, grant it `CONNECT` on this database, `USAGE` on its schema, `SELECT` on `fx_iptv_meta`, `SELECT, UPDATE` on `fx_iptv_catalog` and `fx_iptv_capacity`, and `SELECT, INSERT, UPDATE, DELETE` on `fx_iptv_sessions` and `fx_iptv_operations`. Do not grant schema ownership, migration privileges, `SUPERUSER`, `CREATEDB` or `CREATEROLE`. The current schema has no sequences. Keep the owner account out of the running service.

## Fenced failover

PostgreSQL replication and primary fencing are external deployment requirements. Set a synchronous standby and `synchronous_commit=remote_apply` for the primary. Before admitting viewers, verify the named standby appears with `sync_state=sync` in `pg_stat_replication`; record the primary's flushed WAL position and confirm the standby replayed it. A writable-primary check by FlussoniX proves neither replication nor fencing.

On primary failure, stop viewer admission. Fence the old primary through the deployment's independent power, VM or network control and verify it cannot accept writes. **Do not promote a standby without that evidence.** Verify its replay position includes the last acknowledged WAL, then promote it with PostgreSQL's `pg_ctl promote` or `pg_promote`. Point both authorities at the new primary, have the database owner advance `fx_iptv_meta.epoch` by one, verify their readiness and restore a synchronous standby before reopening admission. A former primary must be rebuilt as a standby, never restarted as another writable primary. Do not treat an asynchronous standby or an unfenced primary as a safe way to preserve strict `max_sessions`.

Keep backups outside the database host with owner-only access because they contain subscriber tokens. Use `pg_dump` with a verified TLS connection and a private password file, then test `pg_restore` into a separate empty database. For an unsupported future schema or a failed upgrade, stop authorities and restore the pre-upgrade backup into a new database; change the credential target only after validation. Do not downgrade tables in place. An older in-memory IPTV authority must not be put in the HA authority pool.

Run the owned local release gate with `scripts/qualify-iptv-ha.sh`. It uses disposable PostgreSQL instances and includes load against both in-process routers and two real HTTPS authority daemons. The daemon gate holds 16 viewer sessions across news and sports throughout the run, renews them repeatedly and checks cross-process session limits. Set `FLUSSONIX_IPTV_HA_LOAD_SECONDS` and `FLUSSONIX_IPTV_HA_DAEMON_LOAD_SECONDS` to choose each load duration (5–900 seconds; 60 by default). The tests check synchronous replay followed by old-primary fencing and standby promotion, and report acquire/renew latency, renewals, session counts, CPU, RSS, database connections and WAL growth. The gate never edits an existing deployment database. Its fixture proof is not a substitute for qualifying the production fencing controller and database topology.

To repeat the 30,000-subscriber catalog comparison, run `FLUSSONIX_IPTV_HA_DAEMON_LOAD_CATALOG_SUBSCRIBERS=30000 scripts/qualify-iptv-ha.sh`. The real-daemon result includes catalog bytes and sampled PostgreSQL backend CPU seconds. This CPU figure covers the database backends observed during the run; it does not include PostgreSQL background workers or establish capacity for your deployment hardware.
