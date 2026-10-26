#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
: "${FLUSSONIX_IPTV_HA_LOAD_SECONDS:=60}"
export FLUSSONIX_IPTV_HA_LOAD_SECONDS
: "${FLUSSONIX_IPTV_HA_DAEMON_LOAD_SECONDS:=$FLUSSONIX_IPTV_HA_LOAD_SECONDS}"
export FLUSSONIX_IPTV_HA_DAEMON_LOAD_SECONDS

pg_fixture_ready() {
  local bindir=$1 sharedir=$2 tool
  [[ -f "$sharedir/postgres.bki" ]] || return 1
  for tool in initdb postgres pg_basebackup pg_dump pg_restore; do
    [[ -x "$bindir/$tool" ]] || return 1
  done
}

pg_bindir=${FLUSSONIX_TEST_PG_BINDIR:-}
pg_sharedir=${FLUSSONIX_TEST_PG_SHAREDIR:-}
if [[ -z "$pg_bindir" ]]; then
  if command -v pg_config >/dev/null 2>&1; then
    if config_bindir=$(pg_config --bindir 2>/dev/null) &&
       config_sharedir=$(pg_config --sharedir 2>/dev/null) &&
       pg_fixture_ready "$config_bindir" "$config_sharedir"; then
      pg_bindir=$config_bindir
      pg_sharedir=$config_sharedir
    fi
  fi
  if [[ -z "$pg_bindir" ]]; then
    for version in 17 16; do
      if pg_fixture_ready "/usr/lib/postgresql/$version/bin" "/usr/share/postgresql/$version"; then
        pg_bindir="/usr/lib/postgresql/$version/bin"
        pg_sharedir="/usr/share/postgresql/$version"
        break
      fi
    done
  fi
fi
if [[ -z "$pg_bindir" ]] || ! pg_fixture_ready "$pg_bindir" "$pg_sharedir"; then
  printf '%s\n' 'IPTV HA gate requires native PostgreSQL test binaries and postgres.bki; set FLUSSONIX_TEST_PG_BINDIR and FLUSSONIX_TEST_PG_SHAREDIR to matching absolute directories.' >&2
  exit 2
fi
export FLUSSONIX_TEST_PG_BINDIR=$pg_bindir FLUSSONIX_TEST_PG_SHAREDIR=$pg_sharedir

cargo test --locked --test iptv_ha_db_failover -- --test-threads=1
cargo test --locked --test iptv_ha_daemons -- --test-threads=1
cargo test --locked --test iptv_ha_load \
  -- --ignored --exact two_authorities_sustain_competing_channels_without_extra_grants --nocapture
cargo test --locked --test iptv_ha_daemons \
  -- --ignored --exact two_real_authority_processes_sustain_competing_channels --nocapture
