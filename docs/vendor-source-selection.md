# Saved vendor source profiles

Explicit vendor profiles now select [guarded daemon HTTP playback](vendor-daemon-playback.md).
The native discovery path skips them. The [source editor](vendor-source-editor.md)
provides ordinary fields for source type, endpoints, TLS trust and local viewer
policies, including group overrides.

Source rows may set `flussonix_source_kind` to `native` or `flussonic`. Omitting it
keeps existing native FlussoniX discovery, including URL-keyed sources. A URL alone
does not select Flussonic management APIs or credentials.

A minimal vendor profile for an explicit coordinator caller is:

```json
{
  "url": "m4ss://origin.example:8443",
  "flussonix_source_kind": "flussonic",
  "cluster_key": "replace-with-the-origin-cluster-key",
  "on_play": "true"
}
```

`on_play: "true"` explicitly permits viewers without a callback. Without an
applicable callback, explicit allow policy or `flussonix_token_sha256` guard, viewer
authority stays unresolved. Configure a token guard alone, or a supported callback
/ `auth://` backend with an optional token guard. Privileged Cluster credentials never
substitute for a viewer policy. Only the explicit vendor kind permits these local
source policy fields and `group_config`, for example:

```json
{
  "on_play": "auth://billing",
  "group_config": {
    "free": { "on_play": "true" },
    "paid": { "flussonix_token_sha256": "<64 hexadecimal characters>" }
  }
}
```

Groups are policy-only objects: `on_play`, `flussonix_token_sha256` and an optional
`name` equal to the group key. Matching groups override source fields; conflicting
matching fields fail. Unmatched groups retain the explicit source policy. Missing
applicable policy stays unresolved. Remote discovery does not supply viewer policy.
Templates in group objects and native-only settings are rejected, including
`drain`, `public_payload_url` and `flussonix_source_group`. They are not silently
discarded or interpreted as vendor failover/placement controls.

The strict profile accepts M4F/M4FS/M4S/M4SS URL identities, optional separate
management/private HTTP(S) roots and independent TLS CA files. The derived
transport must agree with the URL identity. ConfigStore validation and coordinator
preparation use the same complete profile projection. Validation builds clients
and checks local trust files without contacting an origin. All unused local group
policies and referenced backends validate too. Error messages omit endpoint and
credential values.

POST configuration validation, PUT replacement/source updates, backend deletion
and file reopening use this gate. Failed transactions leave configuration, disk
and revision unchanged. At most 64 vendor sources may be configured, including
on-demand rows. Switching to native requires explicitly removing vendor policy
fields; the system never drops guards automatically. Existing coordinator config
revision/source/identity and local-stream-precedence fences remain in force.

Native lookup and inventory reject vendor or malformed type claims before sending
peer headers; native activation and source selection skip vendor profiles. Owned
HTTP tests exercise this isolation, persisted policies through the actual App
authorizer, source/group token guards, unresolved policy, disabled local-stream
precedence and authenticated CRUD. No live vendor or new hardware qualification is
claimed here.
