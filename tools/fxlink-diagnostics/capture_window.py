"""Local capture completeness, never packet delivery or copy attribution."""
import collections,hashlib,json,os
from pathlib import Path
BUCKET_NS=100_000_000
BUCKET_COUNT=5000
MAX_METADATA_BYTES=1024*1024

def uint(x):return type(x) is int and 0<=x<2**64

def valid_trigger(x):
 return (type(x) is dict and set(x)=={'id','subscription','record','reason','at_ns','origin'}
  and type(x['id']) is str and len(x['id'])==32 and all(c in '0123456789abcdef' for c in x['id'])
  and all(uint(x[k]) for k in ['subscription','record','reason','at_ns'])
  and x['reason'] in (1,2,3) and x['origin'] in ('local','external'))

def write_private(path,value):
 path=Path(path);temporary=Path(str(path)+'.writing')
 fd=os.open(temporary,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
 try:
  with os.fdopen(fd,'w') as f:
   json.dump(value,f,separators=(',',':'));f.write('\n');f.flush();os.fsync(f.fileno())
  os.link(temporary,path)
 finally:temporary.unlink()

def read_metadata(path,trace):
 p=Path(str(path)+'.coverage.json')
 with p.open('rb') as f:data=f.read(MAX_METADATA_BYTES+1)
 assert len(data)<=MAX_METADATA_BYTES and data.endswith(b'\n')
 m=json.loads(data)
 assert type(m) is dict and all(uint(m[k]) for k in ['version','bucket_ns','bucket_count'])
 assert m['version']==1 and m['bucket_ns']==BUCKET_NS and m['bucket_count']==BUCKET_COUNT
 assert type(m['bins']) is list and len(m['bins'])==BUCKET_COUNT
 assert all(type(b) is list and len(b)==4 and all(uint(n) for n in b) for b in m['bins'])
 assert all(uint(m[k]) for k in ['stop_ns','start_unix_ns','out_of_range'])
 assert m['stop_ns']==trace['stop_ns'] and m['start_unix_ns']==trace['start_unix_ns']
 assert valid_trigger(m['trigger'])
 assert m['trace_sha256']==hashlib.sha256(Path(path).read_bytes()).hexdigest()
 return m

def window_coverage(trace,left_ns,right_ns):
 m=trace.get('coverage')
 if not m:return dict(complete=False,reason='legacy or absent interval metadata')
 if not uint(left_ns) or not uint(right_ns) or left_ns>=right_ns:return dict(complete=False,reason='invalid nonempty interval')
 first=left_ns//BUCKET_NS;last=(right_ns-1)//BUCKET_NS
 if last>=BUCKET_COUNT or (last+1)*BUCKET_NS>m['stop_ns'] or m['out_of_range']:
  return dict(complete=False,reason='partial or untracked interval')
 retained=collections.Counter(e['ns']//BUCKET_NS for e in trace['events'])
 bad=[]
 for i in range(first,last+1):
  attempted,recorded,rejected,overwritten=m['bins'][i]
  if attempted!=recorded or rejected or overwritten or retained[i]!=recorded:bad.append(i)
 return dict(complete=not bad,reason='local capture only; no packet absence inference' if not bad else 'lost or inconsistent interval events',first_bucket=first,last_bucket=last,invalid_buckets=bad)
