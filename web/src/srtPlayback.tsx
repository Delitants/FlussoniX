type ListenerInfo = {
 enabled?: boolean; listen?: string; encrypted?: boolean;
 latency_ms?: number; client_limit?: number; clients?: number;
 public_url?: string | null; direct_tokens_allowed?: boolean;
};

function ticketReadiness(info: ListenerInfo | undefined, httpsEnabled: boolean): string {
 if (!info?.enabled || !info.encrypted) return 'Needs encrypted SRT listener';
 if (!info.public_url) return 'Needs public SRT URL';
 if (!httpsEnabled) return 'Needs HTTPS issuer';
 return 'Ready';
}

export function SrtPlaybackInfo({info,httpsEnabled}: {info?: ListenerInfo; httpsEnabled: boolean}) {
 return <section role="region" aria-label="SRT playback" className="padded">
  <h2>SRT playback</h2>
  <dl>
   <dt>Listener</dt><dd className="mono">{info?.enabled ? info.listen : 'Not enabled'}</dd>
   <dt>Transport</dt><dd>{info?.enabled ? info.encrypted ? 'Encrypted' : 'Plaintext' : '—'}</dd>
   <dt>Latency</dt><dd>{info?.enabled ? `${info.latency_ms} ms` : '—'}</dd>
   <dt>Viewer slots</dt><dd>{info?.enabled ? info.client_limit : '—'}</dd>
   <dt>Connected callers</dt><dd>{info?.enabled ? info.clients ?? 0 : '—'}</dd>
   <dt>Public SRT URL</dt><dd className="mono">{info?.public_url || 'Not configured'}</dd>
   <dt>Ticket playback</dt><dd>{ticketReadiness(info,httpsEnabled)}</dd>
   <dt>Direct tokens</dt><dd>{info?.direct_tokens_allowed ? 'Allowed (trusted network only)' : 'Off'}</dd>
  </dl>
  <p className="muted">These are startup options. Use --srt-play-listen, --srt-play-public-url and an HTTPS listener with a certificate to enable ticket playback. Restart this node to change them.</p>
  <p className="muted">--srt-play-allow-direct-token permits reusable viewer tokens in SRT handshakes on a trusted network. Leave it off for tokenless playback. Passphrases are never displayed here.</p>
 </section>;
}

export function SrtPlaybackLink({info,name}: {info?: ListenerInfo; name: string}) {
 if (!info?.enabled || !info.listen) return null;
 const original = `#!::r=${name},m=request,u=YOUR_TOKEN`;
 return <section role="region" aria-label="SRT playback URL">
  <h2>SRT playback</h2>
  <dl>
   <dt>Endpoint</dt><dd className="mono">{info.public_url || 'Set --srt-play-public-url on this node'}</dd>
   <dt>HTTPS ticket request</dt><dd className="mono">POST /flussonix/api/v1/srt-ticket</dd>
   <dt>Request body</dt><dd className="mono">{JSON.stringify({streamid:original,encryption:'required'})}</dd>
   <dt>Returned Stream ID</dt><dd className="mono">{`#!::r=${name},m=request,flussonix_ticket=<one-use UUID>`}</dd>
  </dl>
  <p className="muted">Submit the original viewer token in the HTTPS POST body. Use the returned address and full tokenless Stream ID in your SRT caller. Connect within five seconds and request a fresh ticket for every reconnect.</p>
  <p className="muted">The HTTPS request and SRT connection must arrive from the same public IP. Ticket playback requires an encrypted SRT listener; set the configured client passphrase separately. A visible one-use ticket can still be raced during its short lifetime.</p>
  {info.direct_tokens_allowed && <p className="muted">Direct viewer tokens are enabled for trusted-network compatibility. They expose the reusable token in the SRT handshake.</p>}
 </section>;
}
