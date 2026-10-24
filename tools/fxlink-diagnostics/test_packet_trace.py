import unittest,tempfile,hashlib
from pathlib import Path
from packet_trace import Recorder,read_trace,correlate
class Tests(unittest.TestCase):
 def test_bounded_binary_export_is_private_and_marks_gap(self):
  r=Recorder(2)
  for i in range(4):r.record(1,b'token-secret',[i,0,0])
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'trace';r.close(p);x=read_trace(p);self.assertEqual(x['overwritten'],2);self.assertFalse(x['complete']);self.assertEqual(len(p.read_bytes()),64+2*96);self.assertNotIn(b'token-secret',p.read_bytes());self.assertEqual(p.stat().st_mode&0o777,0o600)
 def fixture(self):
  r=Recorder(32);c=b'c'*16
  for stage,data,nums in [(1,b'plain',[0,0,0]),(2,b'',[0,0,0]),(3,b'plain',[7,2,0]),(4,b'cipher',[7,2,0]),(5,b'cipher',[0,0,0]),(6,b'',[0,0,0]),(8,b'cipher',[0,0,0]),(9,b'cipher',[0,0,0])]:r.record(stage,data,nums,c)
  return r
 def test_only_explicit_shaper_drop_is_a_drop(self):
  r=self.fixture();r.record(18,b'cipher',[3,1,0]);x=correlate([r.view()]);self.assertEqual(x[0]['boundary'],'explicit shaper drop');self.assertEqual(x[0]['copy'],1)
 def test_absence_after_acceptance_remains_unresolved(self):
  self.assertEqual(correlate([self.fixture().view()])[0]['boundary'],'unresolved after UDP acceptance')
 def test_overwrite_prevents_negative_inference(self):
  x=self.fixture().view();x['busy']=1;x['complete']=False;self.assertEqual(correlate([x])[0]['boundary'],'incomplete trace; no absence inference')
 def test_duplicate_cipher_disposition_is_not_single_drop(self):
  r=self.fixture();r.record(18,b'cipher',[3,1,0]);r.record(11,b'cipher',[0,0,0]);self.assertEqual(correlate([r.view()])[0]['boundary'],'duplicate packet disposition; ingress and drop both observed')
 def test_truncated_trace_rejected(self):
  r=self.fixture()
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'trace';r.close(p);p.write_bytes(p.read_bytes()[:-1])
   with self.assertRaises(AssertionError):read_trace(p)
 def test_gso_two_datagrams_are_bound_by_exact_offsets(self):
  r=Recorder(32);c=b'c'*16
  for data in [b'a',b'b']:r.record(1,data,[0,0,0],c)
  r.record(2,b'',[0,0,0],c)
  for pn,off,data,cipher in [(7,0,b'a',b'x'*8),(8,8,b'b',b'y'*8)]:
   r.record(3,data,[pn,2,0],c);r.record(4,cipher,[pn,2,off],c);r.record(5,cipher,[off,0,0],c)
  r.record(6,b'',[0,0,0],c)
  rows=correlate([r.view()]);self.assertEqual([x['cipher'] for x in rows],[hashlib.sha256(x).hexdigest() for x in [b'x'*8,b'y'*8]])
 def test_identical_payload_copies_keep_distinct_packet_numbers(self):
  r=Recorder(32);c=b'c'*16
  for i in range(2):
   for stage,data,nums in [(1,b'a',[0,0,0]),(2,b'',[0,0,0]),(3,b'a',[i,2,0]),(4,bytes([i]),[i,2,0]),(5,bytes([i]),[0,0,0]),(6,b'',[0,0,0])]:r.record(stage,data,nums,c)
  rows=correlate([r.view()]);self.assertEqual([x['copy'] for x in rows],[1,2]);self.assertEqual([x['packet'] for x in rows],[[0,2],[1,2]]);self.assertNotEqual(rows[0]['cipher'],rows[1]['cipher'])
 def test_evicting_sender_does_not_claim_fifo_copy_identity(self):
  r=self.fixture();r.record(1,b'plain',[1,0,0],b'c'*16);rows=correlate([r.view()]);self.assertTrue(all(x['boundary']=='source-copy identity ambiguous' for x in rows))
if __name__=='__main__':unittest.main()
