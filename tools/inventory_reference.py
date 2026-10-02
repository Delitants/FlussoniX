#!/usr/bin/env python3
"""Inventory installed/public schemas without copying vendor implementation code."""
import argparse
import csv
import datetime
import hashlib
import json
from pathlib import Path

METHODS = {"get", "put", "post", "delete", "head", "options", "patch"}
FIRST_TAGS = {"stream", "stream-ops", "template", "config", "cluster", "auth", "session", "monitoring", "iptv"}

def operations(doc):
    return {(method.upper(), path): op for path, item in doc.get("paths", {}).items()
            for method, op in item.items() if method in METHODS}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--installed-root", type=Path, default=Path("/opt/flussonic"))
    parser.add_argument("--current-management", type=Path)
    parser.add_argument("--current-streaming", type=Path)
    parser.add_argument("--current-authorization", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("docs/evidence"))
    args = parser.parse_args()
    base = args.installed_root / "lib/web-1/priv"
    sources = [
        ("installed-management-public", base / "schema-v3-public.json", None),
        ("installed-management-private", base / "schema-v3-private.json", None),
        ("installed-streaming", base / "streaming-private.json", None),
    ]
    optional = [
        ("published-management", args.current_management, "https://flussonic.com/doc/api/reference.json"),
        ("published-streaming", args.current_streaming, "https://flussonic.com/doc/api/streaming.json"),
        ("published-authorization", args.current_authorization, "https://flussonic.com/doc/api/authorization.json"),
    ]
    sources.extend(item for item in optional if item[1] is not None)
    docs, provenance = {}, []
    for name, path, url in sources:
        data = path.read_bytes()
        doc = json.loads(data)
        docs[name] = doc
        provenance.append({
            "id": name, "source_path": str(path), "source_url": url,
            "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
            "info_version": doc.get("info", {}).get("version"),
            "paths": len(doc.get("paths", {})), "operations": len(operations(doc)),
            "component_schemas": len(doc.get("components", {}).get("schemas", {})),
            "server_base_urls": [s.get("url") for s in doc.get("servers", [])],
        })
    comparisons = []
    for installed, published in [
        ("installed-management-public", "published-management"),
        ("installed-streaming", "published-streaming"),
    ]:
        if published not in docs:
            continue
        a, b = operations(docs[installed]), operations(docs[published])
        sa = docs[installed].get("components", {}).get("schemas", {})
        sb = docs[published].get("components", {}).get("schemas", {})
        comparisons.append({
            "installed": installed, "published": published,
            "installed_only_operations": [{"method": m, "path": p} for m, p in sorted(a.keys() - b.keys())],
            "published_only_operations": [{"method": m, "path": p} for m, p in sorted(b.keys() - a.keys())],
            "common_schema_names": len(sa.keys() & sb.keys()),
            "common_schemas_with_different_json": sum(sa[n] != sb[n] for n in sa.keys() & sb.keys()),
            "note": "JSON differences include descriptions and metadata; counts do not prove behavioral incompatibility.",
        })
    report = {
        "generated_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "purpose": "Design inventory; no operation has been implemented or behaviorally verified.",
        "sources": provenance, "comparisons": comparisons,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "schema-inventory.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    public = operations(docs["installed-management-public"])
    rows = []
    for name in ["installed-management-public", "installed-management-private", "installed-streaming", "published-authorization"]:
        if name not in docs:
            continue
        for (method, path), op in sorted(operations(docs[name]).items()):
            if name == "installed-management-private" and (method, path) in public:
                continue
            tags = set(op.get("tags", []))
            if name == "installed-streaming":
                relevant = path in {"/{name}/index.m3u8", "/{name}/index.ts.m3u8", "/{name}/index.fmp4.m3u8", "/{name}/tracks-{tracks}/mono.m3u8", "/{name}/video.m3u8", "/{name}/mpegts", "/tv/playlists/{name}"}
                scope = "first_release_candidate" if relevant else "later_or_dependency_review"
            elif name == "published-authorization":
                scope = "first_release_callback_contract"
            else:
                scope = "first_release_candidate" if tags & FIRST_TAGS else "later_or_dependency_review"
            rows.append({
                "surface": name, "method": method, "path": path,
                "operation_id": op.get("operationId", ""),
                "tags": "|".join(sorted(tags)),
                "declared_status_codes": "|".join(sorted(op.get("responses", {}))),
                "scope": scope, "implementation_status": "not_implemented",
                "verification_status": "schema_observed_only",
            })
    with (args.output_dir / "api-operations.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({"schemas": len(provenance), "operation_rows": len(rows), "output_dir": str(args.output_dir)}))

if __name__ == "__main__":
    main()
