# FXLink between FlussoniX nodes

FXLink is an experimental encrypted UDP transport for FlussoniX-to-FlussoniX
media pulls on long-haul links. It uses forward error correction (FEC) and
bounded repair to recover some packet loss within a chosen buffer. It is
separate from Flussonic-compatible M4S/M4SS; those URLs and protocols are
unchanged. FXLink is not a Flussonic protocol and has not completed sustained
WAN or production capacity qualification. A 15-minute two-stream private-LAN
soak kept both HLS viewers active without imposed loss. A separate local
two-stream, constrained-link test covers one limited fairness case. Neither
establishes performance on a long-haul production link.

## Start a source listener

Give the source node a TLS certificate valid for the hostname that receivers
will use, its private key, and a UDP listener address:

```bash
./bin/flussonix --fxlink-listen 0.0.0.0:55000 \
  --fxlink-cert /etc/flussonix/node.example.pem \
  --fxlink-key /etc/flussonix/node.example.key
```

The listener is off by default. Open the selected UDP port between nodes.
The [installation guide](installation.md) covers the normal configuration,
admin credentials, and native peer key. Keep certificate and key files outside
the release archive.

In **Media → Streams**, add an input such as
`fxlink://node.example:55000/news`. Supply the **FXLink peer key** in its
credential field and a separate **FXLink viewer token** when the source's
viewer policy requires one. The peer key does not bypass viewer authorization.
Set a trusted CA file for a private certificate; TLS verifies the hostname.
The URL requires a nonzero port and stream name, and cannot contain credentials,
query parameters, or a fragment.

For a Cluster source, choose **FlussoniX** and **FXLink** as its transport.
Set its HTTP(S) management URL and separate `fxlink://node.example:55000`
private-media endpoint. Discovery still uses the management URL. The Cluster
key supplies peer authentication, and private-media CA trust can be configured
separately. A failed FXLink connection never silently downgrades to plaintext
or another protocol.

## Buffer, FEC, and versions

The **Transport buffer** accepts 500–1,500 ms and defaults to 1,000 ms. It
limits reordering and repair time; it is not total camera-to-viewer latency.
FEC presets trade bandwidth for loss tolerance:

| Preset | Full-block shards | Coding parity |
| --- | --- | --- |
| Low | 20 data + 2 parity | 10% |
| Standard (default) | 20 + 4 | 20% |
| High | 20 + 6 | 30% |
| Short records | 1 + 1 | 100% |

Headers, encryption, control, and repair traffic add overhead. Small shortened
blocks can have less parity than the full-block ratio. FEC cannot replace
missing link capacity or guarantee recovery from sustained loss. Repair remains
bounded by the original media deadline and sender allowance.

The default peers negotiate the highest common built-in FXLink version. Optional
features require matching startup settings on both nodes and default to off:

| Feature | Startup option | Also requires |
| --- | --- | --- |
| Trimmed small records (v6) | `--fxlink-trimmed-records` | None |
| Parity repair (v7) | `--fxlink-repair-parity` | v6 |
| Bootstrap repair (v8) | `--fxlink-bootstrap-repair` | v6 and v7 |
| Record-shape hints (v9) | `--fxlink-geometry-hints` | v6, v7, and v8 |

Restart after changing these options. Check `fxlink.protocol_version` in
stream statistics to see what actually negotiated. Older peers retain their
highest common FXLink version; none of these options changes M4S/M4SS.
Experimental v9 hints can add retry cost and do not extend media deadlines.

## Limits and operation

Accounted transport state is capped at 32 MiB per subscription, 128 MiB per
remote IP, and 512 MiB per process. Nodes behind the same NAT share the remote
IP allowance. These caps exclude the OS, codec workers, and other application
memory. Resource or deadline exhaustion closes the affected subscription
rather than extending the buffer. Check stream statistics for RTT, loss, FEC
recovery, expired records, and charged state. FXLink egress counts application
datagram bytes accepted into QUIC, not physical NIC or IP overhead.

Test the intended stream bitrate, RTT, loss pattern, competing streams, and
viewer behavior on your own link before using FXLink for critical delivery.
A healthy TLS connection or short FEC test does not establish sustained
playback under congestion.

The local shared-connection qualification sends two independent 125-record/s
sources through a 100 ms RTT UDP proxy, with 4,500 media bytes per record and
a 500 ms FXLink buffer. At 15.5 Mb/s, both delivered 400 records in each of
ten repeats, without proxy drops or expired records; the largest sampled
delivery gap was 17 records. A shorter 12.5 Mb/s overload burst delivered 200
records per source in each of ten repeats; its largest gap was 42 records.
The longer 125-record/s load exceeds 12.5 Mb/s after transport overhead and
cannot be sustained at that rate. These loopback results do not qualify WAN
loss, long-running congestion, or a production link's usable capacity.
