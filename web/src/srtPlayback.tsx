type ListenerInfo = {
 enabled?: boolean; listen?: string; encrypted?: boolean;
 latency_ms?: number; client_limit?: number; clients?: number;
};

export function SrtPlaybackInfo({info}: {info?: ListenerInfo}) {
 return <section role="region" aria-label="SRT playback" className="padded">
  <h2>SRT playback</h2>
  <dl>
   <dt>Listener</dt><dd className="mono">{info?.enabled ? info.listen : 'Not enabled'}</dd>
   <dt>Transport</dt><dd>{info?.enabled ? info.encrypted ? 'Encrypted' : 'Plaintext' : '—'}</dd>
   <dt>Latency</dt><dd>{info?.enabled ? `${info.latency_ms} ms` : '—'}</dd>
   <dt>Viewer slots</dt><dd>{info?.enabled ? info.client_limit : '—'}</dd>
   <dt>Connected callers</dt><dd>{info?.enabled ? info.clients ?? 0 : '—'}</dd>
  </dl>
  <p className="muted">The shared listener and encryption are startup options. Restart this node to change them. Passphrases are never displayed here.</p>
 </section>;
}

export function SrtPlaybackLink({info,name}: {info?: ListenerInfo; name: string}) {
 if (!info?.enabled || !info.listen) return null;
 const match = /^(\[[^\]]+\]|[^:]+):(\d+)$/.exec(info.listen);
 if (!match) return null;
 const host = ['0.0.0.0','[::]'].includes(match[1]) ? location.hostname : match[1];
 const endpoint = `srt://${host.includes(':') && !host.startsWith('[') ? `[${host}]` : host}:${match[2]}`;
 return <section role="region" aria-label="SRT playback URL">
  <h2>SRT playback</h2>
  <dl><dt>Endpoint</dt><dd className="mono">{endpoint}</dd>
   <dt>Stream ID</dt><dd className="mono">{`#!::r=${name},m=request,u=YOUR_TOKEN`}</dd>
  </dl>
  <p className="muted">Connect in caller mode. Set the endpoint and Stream ID in your player, replacing YOUR_TOKEN with your viewer token. {info.encrypted ? 'Set the configured client passphrase separately; matching encryption is required.' : 'This listener uses plaintext transport.'}</p>
  <p className="muted">Playback joins the live stream; video may appear at the next keyframe. Use this node’s reachable address if the listener is bound to a private interface.</p>
 </section>;
}
