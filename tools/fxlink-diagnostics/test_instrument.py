import tempfile,unittest
from pathlib import Path
from instrument import validate_output
class Tests(unittest.TestCase):
 def test_output_cannot_recurse_inside_checkout(self):
  with tempfile.TemporaryDirectory() as d:
   repo=Path(d)/'repo';repo.mkdir()
   with self.assertRaises(AssertionError):validate_output(repo,repo/'src'/'private-copy')
 def test_existing_output_is_rejected(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)
   with self.assertRaises(AssertionError):validate_output(p/'repo',p)
 def test_new_sibling_output_allowed(self):
  with tempfile.TemporaryDirectory() as d:validate_output(Path(d)/'repo',Path(d)/'owned-staging')
if __name__=='__main__':unittest.main()
