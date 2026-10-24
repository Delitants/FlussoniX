"""Actual private scheduler and loopback UDP controls; no media qualification."""
import asyncio,collections,heapq,importlib.util,os,random,sys,types,unittest
from pathlib import Path
from packet_trace import Recorder,SHAPER_SEEN,SHAPER_DROP,SHAPER_SENT
class ProxyBase:
 def __init__(self,upstream,profile,seed):
  self.profile=profile;self.random={d:random.Random(seed+i) for i,d in enumerate(['up','down'])};self.heap=[];self.bytes=0;self.serial=0;self.peak=0;self.wake=asyncio.Event();self.error=None;self.bursts=False;self.last_sent=dict(up=-1,down=-1)
  self.stats={d:dict(seen=0,seen_bytes=0,dropped=0,burst_dropped=0,sent=0,sent_bytes=0,reordered=0,delay_min_ms=None,delay_max_ms=0,lateness_max_ms=0) for d in ['up','down']}
class Receive(asyncio.DatagramProtocol):
 def __init__(self):self.q=asyncio.Queue()
 def datagram_received(self,data,addr):self.q.put_nowait(data)
class Tests(unittest.IsolatedAsyncioTestCase):
 async def run_case(self,loss,reorder=False):
  sys.modules['helpers']=types.SimpleNamespace(Proxy=ProxyBase)
  source=Path(os.environ['FX_SHAPER_SOURCE']);spec=importlib.util.spec_from_file_location('private_shaper',source);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
  loop=asyncio.get_running_loop();receive=Receive();rt,_=await loop.create_datagram_endpoint(lambda:receive,local_addr=('127.0.0.1',0));tx,_=await loop.create_datagram_endpoint(asyncio.DatagramProtocol,local_addr=('127.0.0.1',0))
  shaper=mod.Shaper(('127.0.0.1',1),dict(delay_ms=5,jitter_ms=0,loss=loss,burst_ms=0),19);shaper.packet_trace=Recorder(32)
  payloads=[bytes([i])*482 for i in range(3)]
  for data in payloads:shaper.schedule(data,tx,rt.get_extra_info('sockname'),'down')
  if reorder:
   # Change only the test's delivery order; the real scheduler processes its own heap.
   scheduled=sorted(shaper.heap,key=lambda x:x[1]);now=loop.time();shaper.heap=[]
   for i,item in enumerate(scheduled):heapq.heappush(shaper.heap,(now+(3-i)*.004,*item[1:]))
  task=asyncio.create_task(shaper.forward())
  try:
   if loss:
    await asyncio.sleep(.03);self.assertTrue(receive.q.empty())
   else:
    got=[await asyncio.wait_for(receive.q.get(),1) for _ in payloads];self.assertEqual(got,list(reversed(payloads)) if reorder else payloads)
   stages=collections.Counter(e['stage'] for e in shaper.packet_trace.events)
   self.assertEqual(stages[SHAPER_SEEN],3);self.assertEqual(stages[SHAPER_DROP] if loss else stages[SHAPER_SENT],3)
   self.assertEqual(shaper.stats['down']['burst_dropped'],0)
   if reorder:self.assertGreater(shaper.stats['down']['reordered'],0)
  finally:
   task.cancel();await asyncio.gather(task,return_exceptions=True);tx.close();rt.close();await asyncio.sleep(0)
 async def test_actual_delivery(self):await self.run_case(0)
 async def test_explicit_iid_drop(self):await self.run_case(1)
 async def test_actual_reordering(self):await self.run_case(0,True)
if __name__=='__main__':unittest.main()
