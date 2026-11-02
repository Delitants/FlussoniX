import {useEffect, useState} from 'react';
import type {Item} from './forms';

type Props = {
 stream: Item;
 managementRole: string;
 request: (url: string, method?: string, body?: unknown) => Promise<unknown>;
 onRefresh: () => Promise<void>;
};

function protocol(input: Item): string {
 const url = input?.url;
 if (typeof url !== 'string') return 'Configured input';
 const scheme = /^([a-z][a-z0-9+.-]*):\/\//i.exec(url)?.[1];
 return scheme ? scheme.toUpperCase() : 'Configured input';
}

function streamPath(name: string): string {
 return name.includes('://') ? encodeURIComponent(name) : name.split('/').map(encodeURIComponent).join('/');
}

export function InputSelector({stream, managementRole, request, onRefresh}: Props) {
 const [pending, setPending] = useState<number | null>(null);
 const [error, setError] = useState('');
 useEffect(() => {setPending(null); setError('');}, [stream.name]);
 const inputs: Item[] = Array.isArray(stream.inputs) ? stream.inputs : [];
 const active = typeof stream.stats?.input_index === 'number' ? stream.stats.input_index : null;
 const preferred = typeof stream.stats?.preferred_input_index === 'number' ? stream.stats.preferred_input_index : null;
 const publisher = inputs.some(input => input?.url === 'publish://');
 const editable = managementRole === 'edit' && !publisher && !stream.disabled;

 async function select(index: number) {
  setPending(index);
  setError('');
  try {
   await request(`/streamer/api/v3/streams/${streamPath(stream.name)}/inputs/${index}/select`, 'POST');
   await onRefresh();
  } catch (failure) {
   setError((failure as Error).message || 'Input selection failed.');
  } finally {
   setPending(null);
  }
 }

 return <section className="input-selector" role="region" aria-label="Stream input selection">
  <h2>Configured inputs</h2>
  <p className="muted">Choose which source to try first. Automatic fallback remains available. This does not change saved configuration.</p>
  {error && <p className="error" role="alert">{error}</p>}
  {inputs.length ? <div className="input-selection-list">{inputs.map((input, index) => {
   const state = active === index ? `Active input ${index + 1}` : preferred === index ? `Pending input ${index + 1}` : 'Standby';
   return <div className="input-selection-row" key={index}>
    <div><strong>Input {index + 1}</strong><span className="tag">{protocol(input)}</span><small>{state}</small></div>
    {editable && <button type="button" disabled={pending !== null} onClick={() => void select(index)} aria-label={`Select input ${index + 1}`}>{pending === index ? 'Selecting…' : 'Select'}</button>}
   </div>;
  })}</div> : <p className="muted">No inputs configured.</p>}
  {publisher && <p className="muted">This stream receives published media; its input cannot be selected here.</p>}
  {managementRole === 'view' && <p className="muted">View-only access.</p>}
 </section>;
}
