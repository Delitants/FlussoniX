"""Behavioral regressions for the source and release publication guard."""

import gzip
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "publication_guard", ROOT / ".github/publication/guard.py"
)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)
RELEASE_POLICY = {"release_assets": [
    "flussonix-*-linux-x86_64*.tar.gz",
    "flussonix-*-linux-x86_64*.tar.gz.sha256",
    "SHA256SUMS",
]}
PACKAGE_NAME = "flussonix-1-linux-x86_64-abc.tar.gz"


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

    def test_release_archive_accepts_nested_pax_gzip_tar(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            content = b"# Installation\n"
            with tarfile.open(archive, "w:gz", format=tarfile.PAX_FORMAT) as output:
                member = tarfile.TarInfo("flussonix-test/docs/installation.md")
                member.size = len(content)
                output.addfile(member, io.BytesIO(content))
            self.assertEqual([], guard.archive_errors(archive))

    def test_release_archive_rejects_hidden_tar_after_end_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            visible = io.BytesIO()
            with tarfile.open(fileobj=visible, mode="w") as output:
                output.addfile(tarfile.TarInfo("flussonix-test/"))
            hidden = io.BytesIO()
            with tarfile.open(fileobj=hidden, mode="w") as output:
                content = b"private release material\n"
                member = tarfile.TarInfo("private/notes.txt")
                member.size = len(content)
                output.addfile(member, io.BytesIO(content))
            archive.write_bytes(gzip.compress(visible.getvalue() + hidden.getvalue()))
            self.assertTrue(any(
                "nonzero data after tar end marker" in error
                for error in guard.archive_errors(archive)
            ))

    def test_release_archive_rejects_zip_disguised_as_gzip_tar(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as output:
                member = zipfile.ZipInfo("flussonix-test/docs/link")
                member.create_system = 3
                member.external_attr = 0o120777 << 16
                output.writestr(member, "installation.md")
            self.assertTrue(guard.archive_errors(archive))

    def test_release_archive_rejects_plain_tar_disguised_as_gzip_tar(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            with tarfile.open(archive, "w") as output:
                output.addfile(tarfile.TarInfo("flussonix-test/"))
            self.assertTrue(guard.archive_errors(archive))

    def test_release_archive_rejects_invalid_gzip(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            archive.write_bytes(b"not an archive")
            self.assertTrue(guard.archive_errors(archive))

    def test_release_archive_rejects_missing_gzip_trailer(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                output.addfile(tarfile.TarInfo("flussonix-test/"))
            archive.write_bytes(archive.read_bytes()[:-8])
            self.assertTrue(guard.archive_errors(archive))

    def test_release_archive_rejects_corrupt_gzip_crc(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                output.addfile(tarfile.TarInfo("flussonix-test/"))
            damaged = bytearray(archive.read_bytes())
            damaged[-8] ^= 0xFF
            archive.write_bytes(damaged)
            self.assertTrue(guard.archive_errors(archive))

    def test_release_archive_rejects_excessive_uncompressed_data(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                output.addfile(tarfile.TarInfo("flussonix-test/"))
            with mock.patch.object(guard, "MAX_ARCHIVE_UNCOMPRESSED", 1024), \
                    mock.patch.object(guard.tarfile, "open", side_effect=AssertionError("tar parser reached")):
                errors = guard.archive_errors(archive)
            self.assertTrue(any("exceeds uncompressed size limit" in error for error in errors))

    def test_release_archive_accepts_exact_uncompressed_size_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                output.addfile(tarfile.TarInfo("flussonix-test/"))
            with gzip.open(archive, "rb") as source:
                expanded_size = len(source.read())
            with mock.patch.object(guard, "MAX_ARCHIVE_UNCOMPRESSED", expanded_size):
                self.assertEqual([], guard.archive_errors(archive))

    def test_release_archive_rejects_more_than_4096_members(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                for index in range(4097):
                    output.addfile(tarfile.TarInfo(f"flussonix-test/f{index:04d}.bin"))
            self.assertTrue(guard.archive_errors(archive))

    def test_release_archive_rejects_private_tar_link(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / "flussonix-test-linux-x86_64.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                member = tarfile.TarInfo("flussonix-test/docs/link")
                member.type = tarfile.SYMTYPE
                member.linkname = "work/private.log"
                output.addfile(member)
            self.assertTrue(guard.archive_errors(archive))

    def test_release_files_accepts_packager_checksum_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            sidecar = pathlib.Path(directory) / (PACKAGE_NAME + ".sha256")
            sidecar.write_text("a" * 64 + "  " + PACKAGE_NAME + "\n")
            self.assertEqual([], guard.release_errors(RELEASE_POLICY, "v1", [sidecar]))

    def test_release_files_bind_uploaded_archive_to_its_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / PACKAGE_NAME
            with tarfile.open(archive, "w:gz") as output:
                output.addfile(tarfile.TarInfo("flussonix-test/"))
            checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
            sidecar = pathlib.Path(str(archive) + ".sha256")
            sidecar.write_text(f"{checksum}  {PACKAGE_NAME}\n")
            self.assertEqual([], guard.release_errors(RELEASE_POLICY, "v1", [archive, sidecar]))

            sidecar.write_text(f"{'0' * 64}  {PACKAGE_NAME}\n")
            self.assertTrue(any("checksum mismatch" in error for error in
                                guard.release_errors(RELEASE_POLICY, "v1", [archive, sidecar])))

            sidecar.write_text(f"{checksum}  flussonix-other-linux-x86_64.tar.gz\n")
            self.assertTrue(any("checksum" in error for error in
                                guard.release_errors(RELEASE_POLICY, "v1", [archive, sidecar])))

            self.assertTrue(any("checksum" in error for error in
                                guard.release_errors(RELEASE_POLICY, "v1", [archive])))

    def test_release_files_bind_sha256sums_to_uploaded_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = pathlib.Path(directory) / PACKAGE_NAME
            with tarfile.open(archive, "w:gz") as output:
                output.addfile(tarfile.TarInfo("flussonix-test/"))
            checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
            sums = pathlib.Path(directory) / "SHA256SUMS"
            sums.write_text(f"{checksum}  {PACKAGE_NAME}\n")
            self.assertEqual([], guard.release_errors(RELEASE_POLICY, "v1", [archive, sums]))

            sums.write_text(f"{'0' * 64}  {PACKAGE_NAME}\n")
            self.assertTrue(any("checksum mismatch" in error for error in
                                guard.release_errors(RELEASE_POLICY, "v1", [archive, sums])))

            sums.write_text(f"{checksum}  {PACKAGE_NAME}\n{'0' * 64}  {PACKAGE_NAME}\n")
            self.assertTrue(any("checksum" in error for error in
                                guard.release_errors(RELEASE_POLICY, "v1", [archive, sums])))

    def test_release_files_accepts_multiple_checksum_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            sums = pathlib.Path(directory) / "SHA256SUMS"
            sums.write_text(
                "a" * 64 + "  " + PACKAGE_NAME + "\n"
                + "b" * 64 + "  flussonix-1-linux-x86_64-def.tar.gz\n"
            )
            self.assertEqual([], guard.release_errors(RELEASE_POLICY, "v1", [sums]))

    def test_checksum_manifest_may_list_a_release_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            sums = pathlib.Path(directory) / "SHA256SUMS"
            sums.write_text("a" * 64 + "  " + PACKAGE_NAME + ".sha256\n")
            self.assertEqual([], guard.release_errors(RELEASE_POLICY, "v1", [sums]))

    def test_release_files_rejects_zip_bytes_in_checksum_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            sidecar = pathlib.Path(directory) / (PACKAGE_NAME + ".sha256")
            with zipfile.ZipFile(sidecar, "w") as output:
                output.writestr("anything", "contents")
            self.assertTrue(guard.release_errors(RELEASE_POLICY, "v1", [sidecar]))

    def test_release_files_rejects_oversized_checksum_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            sidecar = pathlib.Path(directory) / (PACKAGE_NAME + ".sha256")
            sidecar.write_bytes(b"a" * 65537)
            self.assertTrue(guard.release_errors(RELEASE_POLICY, "v1", [sidecar]))

    def test_release_files_rejects_invalid_checksum_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            sidecar = pathlib.Path(directory) / (PACKAGE_NAME + ".sha256")
            for value in [
                "", "not-hex" + "a" * 57 + "  " + PACKAGE_NAME + "\n",
                "a" * 64 + "  ../private.tar.gz\n",
                "a" * 64 + "  not-a-release.txt\n",
            ]:
                with self.subTest(value=value):
                    sidecar.write_text(value)
                    self.assertTrue(guard.release_errors(RELEASE_POLICY, "v1", [sidecar]))

    def test_release_files_rejects_tar_content_in_checksum_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            sidecar = pathlib.Path(directory) / (PACKAGE_NAME + ".sha256")
            with tarfile.open(sidecar, "w:gz") as output:
                output.addfile(tarfile.TarInfo("flussonix-test/"))
            self.assertTrue(guard.release_errors(RELEASE_POLICY, "v1", [sidecar]))

    def test_invalid_asset_never_reaches_github_upload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            repo = root / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.invalid"], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
            (repo / "tracked").write_text("baseline\n")
            subprocess.run(["git", "-C", str(repo), "add", "tracked"], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "baseline"], check=True)
            baseline = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            policy = root / "policy.json"
            policy.write_text(json.dumps({**RELEASE_POLICY, "baseline": baseline}))
            sidecar = root / (PACKAGE_NAME + ".sha256")
            sidecar.write_text("bad checksum\n")
            fake_bin = root / "bin"
            fake_bin.mkdir()
            marker = root / "github-was-called"
            fake_gh = fake_bin / "gh"
            fake_gh.write_text("#!/bin/sh\nprintf called > \"$GH_MARKER\"\n")
            fake_gh.chmod(0o755)
            result = subprocess.run(
                [sys.executable, "-B", str(ROOT / ".github/publication/guard.py"),
                 "--repository", str(repo), "--policy", str(policy), "upload",
                 "--tag", "v1", "--asset", str(sidecar)],
                capture_output=True, text=True,
                env={**os.environ, "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
                     "GH_MARKER": str(marker)},
            )
            self.assertEqual(1, result.returncode)
            self.assertIn("Publication blocked", result.stderr)
            self.assertFalse(marker.exists())

    def test_upload_requires_authentic_github_origin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            repo = root / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.invalid"], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
            (repo / "tracked").write_text("baseline\n")
            subprocess.run(["git", "-C", str(repo), "add", "tracked"], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "baseline"], check=True)
            baseline = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            policy = root / "policy.json"
            policy.write_text(json.dumps({**RELEASE_POLICY, "baseline": baseline,
                                          "repository": "Delitants/FlussoniX"}))
            sidecar = root / (PACKAGE_NAME + ".sha256")
            sidecar.write_text("a" * 64 + "  " + PACKAGE_NAME + "\n")
            fake_bin = root / "bin"
            fake_bin.mkdir()
            marker = root / "github-was-called"
            fake_gh = fake_bin / "gh"
            fake_gh.write_text("#!/bin/sh\nprintf called > \"$GH_MARKER\"\n"
                               "if [ \"$1\" = api ]; then printf '{\"body\":\"\",\"assets\":[]}'; fi\n")
            fake_gh.chmod(0o755)
            for origin in (
                "https://evil.example/github.com/Delitants/FlussoniX.git",
                "https://github.com@evil.example/github.com/Delitants/FlussoniX.git",
                "https://gıthub.com/Delitants/FlussoniX.git",
                "https://github.com/Delitants/FlussoniX.git ",
            ):
                with self.subTest(origin=origin):
                    subprocess.run(["git", "-C", str(repo), "remote", "remove", "origin"],
                                   check=False, capture_output=True)
                    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", origin], check=True)
                    marker.unlink(missing_ok=True)
                    result = subprocess.run(
                        [sys.executable, "-B", str(ROOT / ".github/publication/guard.py"),
                         "--repository", str(repo), "--policy", str(policy), "upload",
                         "--tag", "v1", "--asset", str(sidecar)],
                        capture_output=True, text=True,
                        env={**os.environ, "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
                             "GH_MARKER": str(marker)},
                    )
                    self.assertEqual(1, result.returncode, result.stderr)
                    self.assertFalse(marker.exists())
            for origin in (
                "https://github.com/Delitants/FlussoniX.git",
                "https://GitHub.com/Delitants/FlussoniX.git",
                "https://alice@github.com/Delitants/FlussoniX.git",
                "https://alice:test-token@github.com/Delitants/FlussoniX.git",
                "git@github.com:Delitants/FlussoniX.git",
                "ssh://git@github.com/Delitants/FlussoniX.git",
            ):
                with self.subTest(origin=origin):
                    subprocess.run(["git", "-C", str(repo), "remote", "set-url", "origin", origin], check=True)
                    marker.unlink(missing_ok=True)
                    result = subprocess.run(
                        [sys.executable, "-B", str(ROOT / ".github/publication/guard.py"),
                         "--repository", str(repo), "--policy", str(policy), "upload",
                         "--tag", "v1", "--asset", str(sidecar)],
                        capture_output=True, text=True,
                        env={**os.environ, "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
                             "GH_MARKER": str(marker)},
                    )
                    self.assertEqual(0, result.returncode, result.stderr)
                    self.assertTrue(marker.exists())

    def test_hook_installer_requires_authentic_github_origin(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = pathlib.Path(directory)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "remote", "add", "origin",
                            "https://evil.example/github.com/Delitants/FlussoniX.git"], check=True)
            for origin in ("https://evil.example/github.com/Delitants/FlussoniX.git",
                           "https://github.com/Delitants/FlussoniX.git "):
                with self.subTest(origin=origin):
                    subprocess.run(["git", "-C", str(repo), "remote", "set-url", "origin", origin], check=True)
                    result = subprocess.run(
                        [sys.executable, "-B", str(ROOT / ".github/publication/install-hooks.py")],
                        cwd=repo, capture_output=True, text=True,
                    )
                    self.assertNotEqual(0, result.returncode)
                    self.assertFalse((repo / ".git/publication-safety-hooks").exists())
            subprocess.run(["git", "-C", str(repo), "remote", "set-url", "origin",
                            "git@github.com:Delitants/FlussoniX.git"], check=True)
            result = subprocess.run(
                [sys.executable, "-B", str(ROOT / ".github/publication/install-hooks.py")],
                cwd=repo, capture_output=True, text=True,
            )
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertTrue((repo / ".git/publication-safety-hooks").is_dir())


if __name__ == "__main__":
    unittest.main()
