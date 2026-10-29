"""Behavioral regressions for the source and release publication guard."""

import importlib.util
import io
import pathlib
import subprocess
import tarfile
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "publication_guard", ROOT / ".github/publication/guard.py"
)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class PublicationGuardTests(unittest.TestCase):
    def test_explicit_public_guide_survives_report_filename_filter(self):
        policy = {"public_docs": ["docs/qualification.md"]}
        self.assertIsNone(guard.private_error("docs/qualification.md", policy))
        self.assertEqual(
            "internal planning/report file",
            guard.private_error("docs/qualification-results.md", policy),
        )
        self.assertEqual(
            "internal planning/report file",
            guard.private_error("docs/qualification.md"),
        )

    def test_upload_artifact_rejects_private_directory_before_another_setting(self):
        workflow = """jobs:
  verify:
    steps:
      - uses: actions/upload-artifact@v4
        with:
          path: work
          if-no-files-found: error
"""
        self.assertTrue(guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_upload_artifact_rejects_quoted_private_directory(self):
        workflow = """jobs:
  verify:
    steps:
      - uses: actions/upload-artifact@v4
        with:
          path: "work"
          retention-days: 1
"""
        self.assertTrue(guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_upload_artifact_rejects_private_directory_in_commented_block_scalar(self):
        workflow = """jobs:
  verify:
    steps:
      - uses: actions/upload-artifact@v4
        with:
          path: | # paths to upload
            dist/bundle.zip
            work
"""
        self.assertTrue(guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_upload_artifact_rejects_private_directory_in_indented_folded_scalar(self):
        workflow = """jobs:
  verify:
    steps:
      - uses: actions/upload-artifact@v4
        with:
          path: >2-
            dist/bundle.zip
            work
"""
        self.assertTrue(guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_upload_artifact_rejects_escaped_private_directory(self):
        workflow = r'''jobs:
  verify:
    steps:
      - uses: actions/upload-artifact@v4
        with:
          path: "\u0077ork"
'''
        self.assertTrue(guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_upload_artifact_rejects_private_path_when_env_is_first_step_key(self):
        workflow = """jobs:
  verify:
    steps:
      - env:
          LABEL: public
        uses: actions/upload-artifact@v4
        with:
          path: work
"""
        self.assertTrue(guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_upload_artifact_rejects_private_path_after_dash_only_step_start(self):
        workflow = """jobs:
  verify:
    steps:
      - uses: actions/upload-artifact@v4
        with:
          path: dist/bundle.zip
      -
        uses: actions/upload-artifact@v4
        with:
          path: work
"""
        self.assertTrue(guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_upload_artifact_rejects_escaped_action_name(self):
        workflow = r'''jobs:
  verify:
    steps:
      - uses: "actions/upload-\u0061rtifact@v4"
        with:
          path: work
'''
        self.assertTrue(guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_upload_artifact_rejects_action_alias_with_private_path(self):
        workflow = """jobs:
  verify:
    steps:
      - uses: &artifact actions/upload-artifact@v4
        with:
          path: dist/bundle.zip
      - uses: *artifact
        with:
          path: work
"""
        self.assertTrue(guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_upload_artifact_allows_private_word_in_name_with_public_path(self):
        workflow = """jobs:
  verify:
    steps:
      - name: Upload work bundle
        uses: actions/upload-artifact@v4
        with:
          name: work
          path: dist/bundle.zip
"""
        self.assertEqual([], guard.workflow_errors(".github/workflows/check.yml", workflow))

    def test_source_history_rejects_internal_text_in_approved_guide(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = pathlib.Path(directory)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.invalid"], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
            guide = repo / "docs/iptv-transport.md"
            guide.parent.mkdir()
            guide.write_text("Public guide\n")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "baseline"], check=True)
            baseline = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            guide.write_text("# Implementation Plan\n")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "public guide"], check=True)
            policy = {"baseline": baseline, "public_docs": ["docs/iptv-transport.md"], "public_data": []}
            self.assertTrue(guard.history_errors(repo, policy, "HEAD"))

    def test_existing_unlisted_guide_is_checked_when_edited(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = pathlib.Path(directory)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.invalid"], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
            guide = repo / "docs/architecture.md"
            guide.parent.mkdir()
            guide.write_text("Public architecture\n")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "baseline"], check=True)
            baseline = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            policy = {"baseline": baseline, "public_docs": [], "public_data": []}
            guide.write_text("# Implementation Plan\n")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            self.assertTrue(any("docs/architecture.md" in error for error in guard.index_errors(repo, policy)))
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "new content"], check=True)
            self.assertTrue(any("docs/architecture.md" in error for error in guard.history_errors(repo, policy, "HEAD")))

    def test_historical_guide_exception_is_bound_to_one_commit_and_path(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = pathlib.Path(directory)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.invalid"], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
            guides = [repo / "docs/one.md", repo / "docs/two.md"]
            guides[0].parent.mkdir()
            for guide in guides:
                guide.write_text("Public guide\n")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "baseline"], check=True)
            baseline = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            for guide in guides:
                guide.write_text("# Implementation Plan\n")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "legacy content"], check=True)
            legacy = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            policy = {"baseline": baseline, "public_docs": [], "public_data": [],
                      "historical_public_guide_exceptions": {legacy: ["docs/one.md"]}}
            errors = guard.history_errors(repo, policy, legacy)
            self.assertEqual(1, len(errors))
            self.assertIn("docs/two.md", errors[0])
            self.assertEqual([], guard.index_errors(repo, policy))
            guides[0].write_text("# Implementation Plan\nNew details\n")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            self.assertTrue(any("docs/one.md" in error for error in guard.index_errors(repo, policy)))
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "new content"], check=True)
            errors = guard.history_errors(repo, policy, "HEAD")
            self.assertTrue(any("docs/one.md" in error for error in errors))

    def test_release_archive_rejects_internal_text_in_packaged_guide(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            content = b"# Implementation Plan\n"
            with tarfile.open(archive, "w:gz") as output:
                member = tarfile.TarInfo("iptv-transport.md")
                member.size = len(content)
                output.addfile(member, io.BytesIO(content))
            self.assertTrue(guard.archive_errors(archive))


if __name__ == "__main__":
    unittest.main()
