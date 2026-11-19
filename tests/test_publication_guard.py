"""Static release-pipeline policy for Debian and protected APT publication."""
import importlib.util
import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    'publication_guard', ROOT / '.github/publication/guard.py'
)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class PublicationWorkflowTests(unittest.TestCase):
    def test_policy_requires_debian_release_triple(self):
        policy = json.loads((ROOT / '.github/publication/policy.json').read_text())
        self.assertIs(policy.get('require_debian_package'), True)
        for pattern in (
            'flussonix_*_amd64.deb', 'flussonix_*_amd64.deb.sha256',
            'flussonix_*_amd64.deb.inspection.json',
        ):
            self.assertIn(pattern, policy['release_assets'])

    def test_ci_candidate_builds_and_uploads_only_archive_and_debian_triple(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        self.assertIn('scripts/package-deb.py', workflow)
        self.assertIn('tests.test_package_deb', workflow)
        self.assertRegex(workflow, r'apt-get[^\n]*install[^\n]*dpkg-dev')
        self.assertIn('name: flussonix-candidate-${{ github.sha }}', workflow)
        self.assertIn('path: release-candidate/', workflow)
        self.assertRegex(workflow, r'\.deb\.inspection\.json')
        self.assertNotRegex(workflow, r'(?i)release-candidate/.*(?:report|plan|log)')

    def test_apt_publication_is_release_only_protected_and_sha_pinned(self):
        path = ROOT / '.github/workflows/apt-publication.yml'
        self.assertTrue(path.is_file())
        workflow = path.read_text()
        self.assertRegex(workflow, r'(?m)^\s*release:\s*$')
        self.assertRegex(workflow, r'(?m)^\s*types:\s*\[published\]\s*$')
        self.assertNotIn('pull_request:', workflow)
        self.assertNotIn('push:', workflow)
        self.assertRegex(workflow, r'(?m)^\s*contents:\s*write\s*$')
        self.assertIn('environment: apt-production', workflow)
        self.assertIn('secrets.APT_SIGNING_SUBKEY_B64', workflow)
        self.assertIn('packaging/apt/signing-key-fingerprint.txt', workflow)
        self.assertIn('.github/publication/guard.py', workflow)
        self.assertIn('git rev-parse HEAD', workflow)
        self.assertIn('scripts/build-apt-repository.py', workflow)
        self.assertIn('pages/apt', workflow)
        self.assertIn('test "${#asset_arguments[@]}" -eq 10', workflow)
        self.assertNotIn('upload-artifact@', workflow)
        self.assertEqual([], guard.workflow_pin_errors(path.as_posix(), workflow))
        secret_line = next(line for line in workflow.splitlines()
                           if 'APT_SIGNING_SUBKEY_B64' in line)
        self.assertIn('secrets.', secret_line)
        self.assertNotRegex(workflow, r'(?m)^\s*run:.*APT_SIGNING_SUBKEY_B64')


if __name__ == '__main__':
    unittest.main()
