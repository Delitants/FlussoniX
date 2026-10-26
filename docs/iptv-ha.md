# IPTV authority high availability

The HA authority profile is being implemented and is not ready to serve viewers in this build. The startup flag remains gated until catalog and lease routing use the shared database. Existing standalone and single-authority IPTV deployments continue to use their current storage and lease behavior.

An HA deployment uses two FlussoniX authority processes backed by one PostgreSQL catalog and lease ledger. PostgreSQL must use verified TLS, synchronous replication, and a qualified failover controller that fences the old primary before promoting a standby. A writable-primary check by FlussoniX alone cannot prove fencing. Asynchronous promotion may lose a recently acknowledged lease and cannot be used for strict `max_sessions`.

Use a dedicated database and runtime account with only `CONNECT` on that database, `USAGE` on its IPTV schema, and the table and sequence privileges required for catalog and lease transactions. Do not give the runtime account `SUPERUSER`, `CREATEDB`, `CREATEROLE`, schema ownership, or migration privileges. Run schema initialization and upgrades with a separate owner account after a backup. Database passwords belong in an owner-only credential file, not command-line arguments, server config or routine logs. Restrict database backups and local catalog exports because they contain subscriber tokens.

The first release will document the exact schema grants, initialization and fenced failover procedure after the database-backed tests pass. Existing Flussonic viewers remain outside FlussoniX subscriber session counts.
