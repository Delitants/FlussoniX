"""Private diagnostic trace format and evidence join, not qualification scoring."""
import collections,hashlib,os,struct,time
from bisect import bisect_left
from pathlib import Path
from capture_window import BUCKET_NS,BUCKET_COUNT,read_metadata,window_coverage,valid_trigger,write_private
SHAPER_SEEN,SHAPER_DROP,SHAPER_SENT,SHAPER_ERROR=17,18,19,20
HEADER=struct.Struct('<8sIIQQQQQQ');EVENT=struct.Struct('<QQHHI16s32sQQQ')
class Recorder:
 def __init__(self,capacity=65536):
  assert 1<=capacity<=131072
  self.capacity=capacity;self.events=collections.deque(maxlen=capacity);self.overwritten=0;self.seq=0;self.start=time.monotonic_ns();self.unix=time.time_ns();self.path=None;self.frozen=None;self.bins=[[0,0,0,0] for _ in range(BUCKET_COUNT)];self.out_of_range=0;self.trigger=None;self.rejected=0
 @classmethod
 def from_env(cls):
  r=cls();r.path=os.environ.get('FLUSSONIX_SHAPER_TRACE');r.enabled=bool(r.path);return r
 def record(self,stage,data,numbers,conn=bytes(16)):
  if self.frozen is not None:return
  ns=time.monotonic_ns()-self.start;i=ns//BUCKET_NS
  if i<BUCKET_COUNT:self.bins[i][0]+=1
  else:self.out_of_range+=1
  if getattr(self,'enabled',True) is False:
   self.rejected+=1
   if i<BUCKET_COUNT:self.bins[i][2]+=1
   return
  if i<BUCKET_COUNT:self.bins[i][1]+=1
  if len(self.events)==self.capacity:
   self.overwritten+=1;old=self.events[0]['ns']//BUCKET_NS
   if old<BUCKET_COUNT:self.bins[old][3]+=1
  self.seq+=1;self.events.append(dict(seq=self.seq,ns=ns,stage=stage,len=len(data),conn=conn.hex(),key=hashlib.sha256(data).hexdigest(),numbers=list(numbers)))
 def view(self):return dict(events=list(self.events),overwritten=self.overwritten,busy=self.rejected,complete=self.overwritten==0 and self.rejected==0,capacity=self.capacity,start_unix_ns=self.unix,stop_ns=self.frozen if self.frozen is not None else time.monotonic_ns()-self.start)
 def freeze(self):
  if self.frozen is None:self.frozen=time.monotonic_ns()-self.start
 def close(self,path=None):
  path=path or self.path
  if path is None:return
  self.freeze()
  x=self.view();fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
  with os.fdopen(fd,'wb') as f:
   f.write(HEADER.pack(b'FXTRACE1',96,1,self.capacity,len(self.events),self.overwritten,self.rejected,self.unix,x['stop_ns']))
   for e in self.events:f.write(EVENT.pack(e['seq'],e['ns'],e['stage'],0,e['len'],bytes.fromhex(e['conn']),bytes.fromhex(e['key']),*e['numbers']))
   f.flush();os.fsync(f.fileno())
 def checkpoint(self,path,trigger):
  assert getattr(self,'enabled',True),'disabled recorder cannot checkpoint'
  assert valid_trigger(trigger)
  if self.trigger is None:self.trigger=dict(trigger)
  self.close(path)
  write_private(str(path)+'.coverage.json',dict(version=1,bucket_ns=BUCKET_NS,bucket_count=BUCKET_COUNT,bins=self.bins,out_of_range=self.out_of_range,stop_ns=self.frozen,start_unix_ns=self.unix,trigger=self.trigger,trace_sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest()))

def read_checkpoint(path):
 trace=read_trace(path);trace['coverage']=read_metadata(path,trace);return trace

def read_trace(path):
 with Path(path).open('rb') as f:b=f.read(64+131072*96+1)
 assert 64<=len(b)<=64+131072*96
 magic,size,flags,capacity,count,over,busy,unix,stop=HEADER.unpack_from(b)
 assert magic==b'FXTRACE1' and size==96 and flags==1 and 1<=capacity<=131072 and count<=capacity and len(b)==64+96*count
 events=[]
 for off in range(64,len(b),96):
  seq,ns,stage,res,n,conn,key,a,c,d=EVENT.unpack_from(b,off);assert res==0 and 1<=stage<=20
  events.append(dict(seq=seq,ns=ns,stage=stage,len=n,conn=conn.hex(),key=key.hex(),numbers=[a,c,d]))
 assert all(a['seq']<b['seq'] for a,b in zip(events,events[1:]))
 return dict(events=events,overwritten=over,busy=busy,complete=not(over or busy),capacity=capacity,start_unix_ns=unix,stop_ns=stop)
def _batch_cipher_index(batch):
 """Index one batch without changing duplicate or inclusive interval semantics."""
 keys=[];packets=[]
 # Sorted numeric keys avoid attacker-controlled numeric hash collisions.
 for packet in sorted(batch['packets'],key=lambda p:p['numbers'][:2]):
  key=tuple(packet['numbers'][:2])
  if keys and keys[-1]==key:packets[-1]=None
  else:keys.append(key);packets.append(packet)
 ciphers=[None]*len(keys)
 queries=sorted((p['numbers'][2],p['numbers'][2]+p['len'],i) for i,p in enumerate(packets) if p is not None)
 if not queries:return keys,ciphers
 segments=sorted(batch['segments'],key=lambda g:g['numbers'][0])
 next_segment=0;ends=[]
 for start,end,index in queries:
  while next_segment<len(segments) and segments[next_segment]['numbers'][0]<=start:
   segment=segments[next_segment];next_segment+=1
   # Only the largest two eligible ends are needed to prove uniqueness.
   # Keep duplicate records, even when their offsets and hashes are identical.
   ends.append((segment['numbers'][0]+segment['len'],segment['key']))
   ends.sort(key=lambda item:item[0],reverse=True);del ends[2:]
  if ends and ends[0][0]>=end and (len(ends)==1 or ends[1][0]<end):
   ciphers[index]=ends[0][1]
 return keys,ciphers

def correlate(traces):
 """Join observed copies. Missing events never establish network loss."""
 complete=all(t['complete'] for t in traces)
 all_events=[(node,e) for node,t in enumerate(traces) for e in t['events']]
 ambiguous_connections={(node,e['conn']) for node,e in all_events if e['stage']==1 and (e['numbers'][0] or e['len']>1100)}
 stages=collections.defaultdict(set)
 for node,e in all_events:stages[e['key']].add(e['stage'])
 batches={};active={};queues=collections.defaultdict(collections.deque);copies=[];ordinals=collections.Counter()
 for node,e in all_events:
  conn=(node,e['conn']);stage=e['stage'];payload=(conn,e['key'])
  if stage==1:
   ordinals[payload]+=1;c=dict(node=node,conn=e['conn'],key=e['key'],copy=ordinals[payload],queue_seq=e['seq'],cipher=None,packet=None,drop_permitted=bool(e['numbers'][0]));copies.append(c);queues[payload].append(c)
  elif stage==2:
   active[conn]=dict(frames=[],packets=[],segments=[])
  elif stage in (3,4,5) and conn in active:
   active[conn][{3:'frames',4:'packets',5:'segments'}[stage]].append(e)
  elif stage==6 and conn in active:
   batch=active.pop(conn);indexed=None
   for f in batch['frames']:
    if not queues[(conn,f['key'])]:continue
    c=queues[(conn,f['key'])].popleft();c['packet']=f['numbers'][:2]
    # No queued frames means no association work for this batch.
    if indexed is None:indexed=_batch_cipher_index(batch)
    keys,ciphers=indexed;key=tuple(c['packet']);at=bisect_left(keys,key)
    if at<len(keys) and keys[at]==key:c['cipher']=ciphers[at]
 for c in copies:
  ss=stages.get(c['cipher'],set());ps=stages[c['key']]
  c['observed_cipher_stages']=sorted(ss);c['observed_payload_stages']=sorted(ps)
  # Packet-local encrypted hashes connect stages; payload hashes alone cannot
  # distinguish identical repair copies after the receiver dequeues one copy.
  if not complete:c['boundary']='incomplete trace; no absence inference'
  elif (c['node'],c['conn']) in ambiguous_connections:c['boundary']='source-copy identity ambiguous'
  elif 18 in ss and 11 in ss:c['boundary']='duplicate packet disposition; ingress and drop both observed'
  elif 18 in ss:c['boundary']='explicit shaper drop'
  elif 11 in ss:c['boundary']='receiver UDP ingress observed; per-copy dispatch not established'
  elif 9 in ss:c['boundary']='unresolved after UDP acceptance'
  elif 8 in ss or 7 in ss:c['boundary']='UDP write delayed; acceptance unobserved'
  elif c['cipher']:c['boundary']='packetized; UDP acceptance unobserved'
  elif c['packet']:c['boundary']='frame observed; encrypted UDP mapping unavailable'
  else:c['boundary']='queue admission observed; packetization unobserved'
 return copies

if __name__=='__main__':
 import argparse,json
 p=argparse.ArgumentParser();p.add_argument('traces',nargs='+',type=Path);a=p.parse_args();traces=[read_trace(t) for t in a.traces];print(json.dumps(dict(trace_complete=all(t['complete'] for t in traces),copies=correlate(traces)),indent=2))

from checkpoint_coordinator import checkpoint_cohort
