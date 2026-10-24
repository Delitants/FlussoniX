"""Owned file-based checkpoint coordination; no media or network peers."""
import asyncio,json,tempfile,time,unittest
from pathlib import Path
import packet_trace
from capture_window import write_private
class Tests(unittest.IsolatedAsyncioTestCase):
 async def case(self,source_responds=True,existing=False,wrong_origin=False):
  self.assertTrue(callable(getattr(packet_trace,'checkpoint_cohort',None)),'cohort checkpoint coordinator is absent')
  with tempfile.TemporaryDirectory() as d:
   d=Path(d);receiver=d/'receiver';source=d/'source';proxy=d/'proxy'
   r=packet_trace.Recorder(32);s=packet_trace.Recorder(32);p=packet_trace.Recorder(32)
   for x in [r,s,p]:x.record(17,b'cipher',[1,1,0])
   trigger=dict(id='cd'*16,subscription=0,record=7,reason=1,at_ns=time.monotonic_ns()-r.start,origin='local');write_private(str(receiver)+'.trigger',trigger)
   if existing:write_private(str(source)+'.request',dict(keep='existing'))
   async def peers():
    await asyncio.sleep(.22);r.checkpoint(receiver,trigger)
    if source_responds:
     while not Path(str(source)+'.request').exists():await asyncio.sleep(.01)
     request=json.loads(Path(str(source)+'.request').read_text());request.update(origin='local' if wrong_origin else 'external',at_ns=time.monotonic_ns()-s.start);s.checkpoint(source,request)
   task=asyncio.create_task(peers())
   try:
    if existing:
     with self.assertRaises(FileExistsError):await packet_trace.checkpoint_cohort(receiver,source,p,proxy,timeout=1)
     self.assertEqual(json.loads(Path(str(source)+'.request').read_text()),dict(keep='existing'));return
    if not source_responds:
     with self.assertRaises(TimeoutError):await packet_trace.checkpoint_cohort(receiver,source,p,proxy,timeout=.5)
     return
    if wrong_origin:
     with self.assertRaises(AssertionError):await packet_trace.checkpoint_cohort(receiver,source,p,proxy,timeout=2)
     return
    result=await packet_trace.checkpoint_cohort(receiver,source,p,proxy,timeout=2)
    self.assertEqual(result['id'],trigger['id']);self.assertEqual(set(result['captures']),{'source','receiver','proxy'});self.assertEqual(len(p.events),1)
    for name,t in result['captures'].items():self.assertEqual(t['trigger']['id'],trigger['id'])
   finally:task.cancel();await asyncio.gather(task,return_exceptions=True)
 async def test_three_owned_roles_checkpoint_same_failure(self):await self.case()
 async def test_missing_source_cannot_qualify(self):await self.case(False)
 async def test_preexisting_request_is_preserved(self):await self.case(existing=True)
 async def test_wrong_source_role_does_not_qualify(self):await self.case(wrong_origin=True)
 async def test_cancel_before_notice_leaves_proxy_recording(self):
  with tempfile.TemporaryDirectory() as d:
   d=Path(d);proxy=packet_trace.Recorder(4)
   task=asyncio.create_task(packet_trace.checkpoint_cohort(d/'receiver',d/'source',proxy,d/'proxy',timeout=2))
   await asyncio.sleep(.02);task.cancel();await asyncio.gather(task,return_exceptions=True)
   proxy.record(17,b'cipher',[1,1,0]);self.assertEqual(len(proxy.events),1);self.assertEqual(list(d.iterdir()),[])
 async def test_present_peers_cannot_finish_after_coordinator_deadline(self):
  with tempfile.TemporaryDirectory() as d:
   d=Path(d);r=packet_trace.Recorder(4);s=packet_trace.Recorder(4);proxy=packet_trace.Recorder(4)
   trigger=dict(id='fe'*16,subscription=0,record=7,reason=1,at_ns=0,origin='local')
   write_private(str(d/'receiver')+'.trigger',trigger);r.checkpoint(d/'receiver',trigger);s.checkpoint(d/'source',dict(trigger,origin='external'))
   began=time.monotonic()
   with self.assertRaises(TimeoutError):await packet_trace.checkpoint_cohort(d/'receiver',d/'source',proxy,d/'proxy',timeout=.02)
   self.assertLess(time.monotonic()-began,.15)
if __name__=='__main__':unittest.main()
