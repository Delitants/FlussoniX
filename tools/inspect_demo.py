#!/usr/bin/env python3
"""Inspect a Flussonic functionality demo; emit only aggregate metadata.

Uses GET /config?full=true and paginated GET /streams. Does not open media,
modify configuration, trigger reauthorization, or persist credentials/raw data.
"""
import argparse
import base64
import collections
import datetime
import getpass
import json
import os
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def scheme(value):
    if isinstance(value, dict):
        value = value.get("url", "")
    return urllib.parse.urlsplit(value or "").scheme if isinstance(value, str) else "unknown"


def count(items):
    return dict(sorted(collections.Counter(items).items()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url")
    parser.add_argument("--username", default="flussonic")
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    parts = urllib.parse.urlsplit(args.base_url)
    if parts.scheme not in ("http", "https") or not parts.netloc or parts.username or parts.password or parts.query or parts.fragment or parts.path not in ("", "/"):
        parser.error("base_url must be an HTTP(S) origin without credentials")
    if args.max_pages < 1:
        parser.error("max-pages must be positive")
    password = os.environ.get("FLUSSONIX_REFERENCE_PASSWORD")
    if password is None:
        password = getpass.getpass("Reference server password: ")
    authorization = "Basic " + base64.b64encode((args.username + ":" + password).encode()).decode()
    opener = urllib.request.build_opener(NoRedirect())
    reads = []

    def get(path, query):
        # Keep the reader incapable of selecting a mutation or a media URL.
        if path not in ("/streamer/api/v3/config", "/streamer/api/v3/streams"):
            raise ValueError("unsupported inventory route")
        url = args.base_url.rstrip("/") + path + "?" + urllib.parse.urlencode(query)
        request = urllib.request.Request(url, headers={"Authorization": authorization, "Accept": "application/json"}, method="GET")
        with opener.open(request, timeout=20) as response:
            body = response.read(16 * 1024 * 1024 + 1)
            if len(body) > 16 * 1024 * 1024:
                raise ValueError("inventory response exceeds 16 MiB")
            reads.append({"method": "GET", "path": path, "status": response.status})
            return json.loads(body)

    config = get("/streamer/api/v3/config", {"full": "true"})
    rows, cursors, cursor = [], set(), None
    estimated = None
    for _ in range(args.max_pages):
        query = {"limit": 100, "select": "name,inputs,stats,dvr,transcoder,on_play,on_publish,static,disabled,protocols,cluster_ingest,named_by"}
        if cursor:
            query["cursor"] = cursor
        page = get("/streamer/api/v3/streams", query)
        rows.extend(page.get("streams", []))
        estimated = page.get("estimated_count", estimated)
        cursor = page.get("next")
        if not cursor:
            break
        if cursor in cursors:
            raise ValueError("reference server repeated a cursor")
        cursors.add(cursor)

    stats = config.get("stats", {})
    inputs = [item for row in rows for item in row.get("inputs", []) if isinstance(item, dict)]
    auth = [row.get("on_play") for row in rows if row.get("on_play")]
    source_configs = config.get("sources", [])
    relevant = ["dvr", "transcoder", "on_play", "on_publish", "disabled", "static", "cluster_ingest"]
    report = {
        "observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "reference_origin": args.base_url.rstrip("/"),
        "method": "Authenticated GET configuration and stream metadata only; sequential pagination; no media requests",
        "server_version": stats.get("server_version"),
        "transcoder_device_summary": [{k: d[k] for k in ["name", "type", "model", "vendor", "encoder", "decoder"] if k in d} for d in stats.get("transcoder_devices", []) if isinstance(d, dict)],
        "stats_snapshot": {k: stats[k] for k in ["uptime", "cpu_usage", "memory_usage", "input_kbit", "output_kbit", "online_streams", "total_streams", "total_clients", "opened_files"] if k in stats},
        "pagination": {"rows": len(rows), "unique_stream_names": len({r.get("name") for r in rows}), "estimated_count": estimated, "complete": not bool(cursor)},
        "config_counts": {k: len(config[k]) for k in ["streams", "sources", "peers", "balancers", "templates", "auth_backends", "dvrs", "vods", "event_sinks"] if isinstance(config.get(k), list)},
        "config_keys_present": sorted(config),
        "config_flags": {k: bool(config.get(k)) for k in ["cluster_key", "config_external", "edit_auth", "view_auth", "auth_token"]},
        "input_schemes": count(scheme(i) for i in inputs),
        "primary_input_schemes": count(scheme(r["inputs"][0]) for r in rows if r.get("inputs")),
        "input_count_per_stream": count(str(len(r.get("inputs", []))) for r in rows),
        "input_cluster_keys_present": sum(bool(i.get("cluster_key")) for i in inputs),
        "input_url_userinfo_present": sum(bool(urllib.parse.urlsplit(i.get("url", "")).username) for i in inputs),
        "input_remote_dvr_present": sum(bool(i.get("remote_dvr")) for i in inputs),
        "stream_flags": {k: sum(bool(r.get(k)) for r in rows) for k in relevant},
        "stream_statuses": count(str(r.get("stats", {}).get("status")) for r in rows),
        "stream_named_by": count(str(r.get("named_by")) for r in rows),
        "play_auth_schemes": count(scheme(a) for a in auth),
        "play_auth_field_names": sorted({k for a in auth if isinstance(a, dict) for k in a}),
        "play_auth_distinct_values": len({json.dumps(a, sort_keys=True) for a in auth}),
        "source_schemes": count(scheme(s) for s in source_configs),
        "source_field_names": sorted({k for s in source_configs if isinstance(s, dict) for k in s}),
        "stream_field_names": sorted({k for r in rows for k in r}),
        "stream_stats_field_names": sorted({k for r in rows for k in r.get("stats", {})}),
        "reads": reads,
        "limitations": ["A point-in-time inventory is not peak capacity evidence.", "Pagination is not an atomic configuration snapshot.", "Stream names, source URLs, backend URLs, tokens, credentials and config bodies are omitted."]
    }
    with open(args.output, "w") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (urllib.error.URLError, ValueError, KeyError, TypeError) as error:
        # Avoid printing request URLs containing opaque pagination cursors.
        raise SystemExit("Inventory failed: " + type(error).__name__)
