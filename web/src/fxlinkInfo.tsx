import type {Item} from './forms';
export function FXLinkInfo({info}:{info?:Item}) {
 return <section role="region" aria-label="FXLink transport" className="padded"><h2>FXLink transport</h2><dl><dt>UDP listener</dt><dd className="mono">{info?.listen||'Not enabled'}</dd><dt>Protection</dt><dd>TLS + Reed–Solomon FEC</dd><dt>Default receive budget</dt><dd>1,000 ms</dd></dl><p className="muted">Enable with the FXLink listener and TLS certificate startup options. Inputs use fxlink:// and verify the source certificate.</p></section>;
}
