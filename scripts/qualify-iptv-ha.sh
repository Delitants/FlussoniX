#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
: "${FLUSSONIX_IPTV_HA_LOAD_SECONDS:=60}"
export FLUSSONIX_IPTV_HA_LOAD_SECONDS
: "${FLUSSONIX_IPTV_HA_DAEMON_LOAD_SECONDS:=$FLUSSONIX_IPTV_HA_LOAD_SECONDS}"
export FLUSSONIX_IPTV_HA_DAEMON_LOAD_SECONDS

cargo test --locked --test iptv_ha_db_failover -- --test-threads=1
cargo test --locked --test iptv_ha_daemons -- --test-threads=1
cargo test --locked --test iptv_ha_load \
  -- --ignored --exact two_authorities_sustain_competing_channels_without_extra_grants --nocapture
cargo test --locked --test iptv_ha_daemons \
  -- --ignored --exact two_real_authority_processes_sustain_competing_channels --nocapture
