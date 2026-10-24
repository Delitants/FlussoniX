"""Coordinate only an owned local diagnostic cohort after its first failure."""
import asyncio,json,time
from pathlib import Path
from capture_window import valid_trigger,write_private,window_coverage

async def checkpoint_cohort(receiver_path,source_path,proxy,proxy_path,timeout=450):
 if timeout<=0:raise TimeoutError('owned checkpoint deadline elapsed')
 async with asyncio.timeout(timeout):
  return await _checkpoint_cohort(receiver_path,source_path,proxy,proxy_path,timeout)

async def _checkpoint_cohort(receiver_path,source_path,proxy,proxy_path,timeout):
 from packet_trace import read_checkpoint
 assert getattr(proxy,'enabled',True) and proxy.frozen is None,'active live proxy recorder required'
 receiver_path=Path(receiver_path);source_path=Path(source_path);proxy_path=Path(proxy_path)
 assert len({receiver_path.resolve(),source_path.resolve(),proxy_path.resolve()})==3
 end=time.monotonic()+timeout
 async def wait_file(path):
  while not path.exists():
   if time.monotonic()>=end:raise TimeoutError('owned checkpoint peer did not finish')
   await asyncio.sleep(.01)
 notice=Path(str(receiver_path)+'.trigger');await wait_file(notice)
 with notice.open('rb') as f:data=f.read(4097)
 assert len(data)<=4096 and data.endswith(b'\n')
 trigger=json.loads(data);assert valid_trigger(trigger) and trigger['origin']=='local'
 await asyncio.to_thread(write_private,str(source_path)+'.request',trigger)
 local=dict(trigger,at_ns=time.monotonic_ns()-proxy.start,origin='external')
 await asyncio.sleep(.2)
 proxy.freeze()
 await asyncio.to_thread(proxy.checkpoint,proxy_path,local)
 captures={}
 for name,path in [('receiver',receiver_path),('source',source_path),('proxy',proxy_path)]:
  await wait_file(Path(str(path)+'.coverage.json'))
  trace=await asyncio.to_thread(read_checkpoint,path);t=trace['coverage']['trigger'];assert t['id']==trigger['id']
  assert all(t[k]==trigger[k] for k in ['subscription','record','reason'])
  assert t['origin']==('local' if name=='receiver' else 'external')
  # Each peer has its own clock and local observation time. These windows are
  # capture-quality checks, not a synchronized end-to-end media deadline.
  window=window_coverage(trace,max(0,t['at_ns']-2_000_000_000),t['at_ns']+100_000_000)
  captures[name]=dict(trigger=t,window=window,global_complete=trace['complete'],events=len(trace['events']))
 return dict(id=trigger['id'],captures=captures,limits='Local capture coverage only. No absence, copy attribution, cross-clock deadline or media acceptance inference.')
