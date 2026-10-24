"""Failure-window evidence controls. These never score packet absence."""
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from packet_trace import Recorder
import packet_trace

NS=100_000_000
TRIGGER=dict(id='ab'*16,subscription=0,record=7,reason=1,at_ns=2*NS,origin='local')
class Tests(unittest.TestCase):
 def recorder(self,capacity=16):
  with patch('packet_trace.time.monotonic_ns',return_value=0):r=Recorder(capacity)
  self.assertTrue(callable(getattr(r,'checkpoint',None)),'private checkpoint API is absent')
  return r
 def add(self,r,at,data=b'cipher'):
  with patch('packet_trace.time.monotonic_ns',return_value=at):r.record(17,data,[1,1,0])
 def dump(self,r,p,at=5*NS):
  with patch('packet_trace.time.monotonic_ns',return_value=at):r.checkpoint(p,TRIGGER)
  return packet_trace.read_checkpoint(p)
 def test_earlier_overwrite_does_not_poison_a_retained_window(self):
  r=self.recorder(2);self.add(r,0,b'old');self.add(r,NS,b'a');self.add(r,2*NS,b'b')
  with tempfile.TemporaryDirectory() as d:
   t=self.dump(r,Path(d)/'capture');self.assertFalse(t['complete']);self.assertTrue(packet_trace.window_coverage(t,NS,3*NS)['complete']);self.assertFalse(packet_trace.window_coverage(t,0,3*NS)['complete'])
 def test_missing_metadata_cannot_qualify_legacy_trace(self):
  r=self.recorder()
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'capture';r.close(p);self.assertFalse(packet_trace.window_coverage(packet_trace.read_trace(p),0,NS)['complete'])
 def test_checkpoint_is_immutable_and_private(self):
  r=self.recorder();self.add(r,NS)
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'capture';t=self.dump(r,p);self.add(r,6*NS,b'new');self.assertEqual(len(r.events),1);self.assertEqual(t['coverage']['trigger'],TRIGGER);self.assertEqual(p.stat().st_mode&0o777,0o600);self.assertEqual(Path(str(p)+'.coverage.json').stat().st_mode&0o777,0o600)
   with self.assertRaises(FileExistsError):r.checkpoint(p,TRIGGER)
 def test_partial_terminal_bucket_is_not_complete(self):
  r=self.recorder();self.add(r,NS)
  with tempfile.TemporaryDirectory() as d:
   t=self.dump(r,Path(d)/'capture',2*NS+1);self.assertTrue(packet_trace.window_coverage(t,NS,2*NS)['complete']);self.assertFalse(packet_trace.window_coverage(t,2*NS,2*NS+1)['complete'])
 def test_metadata_is_bound_to_exact_trace(self):
  r=self.recorder();self.add(r,NS)
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'capture';self.dump(r,p);b=bytearray(p.read_bytes());b[-1]^=1;p.write_bytes(b)
   with self.assertRaises(AssertionError):packet_trace.read_checkpoint(p)
 def test_truncated_metadata_rejected(self):
  r=self.recorder()
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'capture';self.dump(r,p);m=Path(str(p)+'.coverage.json');m.write_bytes(m.read_bytes()[:-1])
   with self.assertRaises((AssertionError,ValueError)):packet_trace.read_checkpoint(p)
 def test_bucket_counter_mismatch_cannot_claim_completeness(self):
  r=self.recorder();self.add(r,NS)
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'capture';self.dump(r,p);m=Path(str(p)+'.coverage.json');x=json.loads(m.read_text());x['bins'][1][1]+=1;m.write_text(json.dumps(x)+'\n')
   self.assertFalse(packet_trace.window_coverage(packet_trace.read_checkpoint(p),NS,2*NS)['complete'])
 def test_invalid_ranges_do_not_qualify(self):
  r=self.recorder()
  with tempfile.TemporaryDirectory() as d:
   t=self.dump(r,Path(d)/'capture')
   for a,b in [(-1,NS),(NS,NS),(2*NS,NS),(False,NS),(0,600_000_000_000)]:self.assertFalse(packet_trace.window_coverage(t,a,b)['complete'])
 def test_busy_outside_window_is_separate_from_busy_inside(self):
  r=self.recorder();self.add(r,2*NS)
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'capture';t=self.dump(r,p)
   # Golden producer metadata: an independently lost attempt in bucket0.
   t['coverage']['bins'][0]=[1,0,1,0];t['busy']=1;t['complete']=False
   self.assertTrue(packet_trace.window_coverage(t,2*NS,3*NS)['complete'])
   self.assertFalse(packet_trace.window_coverage(t,0,NS)['complete'])
 def test_boolean_metadata_header_is_rejected(self):
  r=self.recorder()
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'capture';self.dump(r,p);m=Path(str(p)+'.coverage.json');x=json.loads(m.read_text());x['version']=True;m.write_text(json.dumps(x)+'\n')
   with self.assertRaises(AssertionError):packet_trace.read_checkpoint(p)
 def test_disabled_capture_cannot_export_complete_checkpoint(self):
  r=self.recorder();r.enabled=False;self.add(r,NS)
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'capture'
   with self.assertRaises(AssertionError):self.dump(r,p)
   self.assertFalse(p.exists())
 def test_reenabled_recorder_preserves_disabled_attempt_gaps(self):
  r=self.recorder();r.enabled=False;self.add(r,0);r.enabled=True;self.add(r,NS)
  with tempfile.TemporaryDirectory() as d:
   t=self.dump(r,Path(d)/'capture');self.assertFalse(t['complete']);self.assertFalse(packet_trace.window_coverage(t,0,NS)['complete']);self.assertTrue(packet_trace.window_coverage(t,NS,2*NS)['complete'])
if __name__=='__main__':unittest.main()
