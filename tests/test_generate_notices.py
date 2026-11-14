"""Deterministic source selection for the release's third-party texts."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/generate-third-party-notices.py'


def module():
    if not SCRIPT.is_file():
        raise AssertionError('notice generator missing')
    spec = importlib.util.spec_from_file_location('generate_notices', SCRIPT)
    assert spec is not None and spec.loader is not None
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class GenerateNoticesTests(unittest.TestCase):
    def test_tree_parser_deduplicates_both_linux_targets_and_excludes_root(self):
        generate = module()
        x86 = 'flussonix v0.10.0 (/project)\naxum v0.8.9\nring v0.17.14 (*)\n'
        arm = 'flussonix v0.10.0 (/project)\naxum v0.8.9 (*)\n'
        self.assertEqual(generate.parse_tree(x86, arm),
                         {('axum', '0.8.9'), ('ring', '0.17.14')})

    def test_recursive_license_texts_include_nested_notices_but_no_source_code(self):
        generate = module()
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            (source / 'LICENSE-MIT').write_text('MIT terms')
            (source / 'nested').mkdir()
            (source / 'nested/NOTICE.txt').write_text('nested attribution')
            (source / 'nested/ordinary.rs').write_text('source code')
            self.assertEqual([p.as_posix() for p in generate.license_paths(source)],
                             ['LICENSE-MIT', 'nested/NOTICE.txt'])

    def test_linked_license_source_rejected(self):
        generate = module()
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            outside = source / 'outside.txt'
            outside.write_text('not a license')
            (source / 'LICENSE').symlink_to(outside)
            with self.assertRaisesRegex(ValueError, 'linked'):
                generate.license_paths(source)

    def test_alternate_registry_cannot_substitute_cached_crates_io_source(self):
        generate = module()
        self.assertTrue(hasattr(generate, 'rust_source'), 'rust source selection is missing')
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cached = root / 'cargo/registry/src/index/axum-0.8.9'
            cached.mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, 'registry'):
                generate.rust_source(root, root / 'cargo', 'axum', '0.8.9',
                                     'registry+https://alternate.example/index')


if __name__ == '__main__':
    unittest.main()
