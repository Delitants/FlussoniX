"""Boundary tests for disposable APT lifecycle qualification."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/qualify-apt-install.py'


def load_runner():
    if not SCRIPT.is_file():
        raise FileNotFoundError(SCRIPT)
    spec = importlib.util.spec_from_file_location('flussonix_apt_qualifier', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RecordingAdapter:
    def __init__(self, failure=None):
        self.failure = failure
        self.calls = []

    def run(self, operation):
        self.calls.append(operation)
        if self.failure == operation.phase:
            raise RuntimeError('injected qualification failure')
        if self.failure == 'interrupt' and operation.phase == 'probe':
            raise KeyboardInterrupt()
        if operation.kind.startswith('wait_'):
            return subprocess.CompletedProcess(operation.argv, 0, 'running\n', '')
        if Path(operation.argv[0]).name == 'machinectl' and 'show' in operation.argv:
            return subprocess.CompletedProcess(operation.argv, 1, '', 'not found')
        return subprocess.CompletedProcess(operation.argv, 0, 'ok\n', '')


class AptQualificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='flussonix-qualifier-test-')
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.repository = self.directory / 'repository'
        index = self.repository / 'dists/noble/main/binary-amd64/Packages'
        index.parent.mkdir(parents=True)
        self.repository.joinpath('dists/noble/Release').write_text(
            'Origin: FlussoniX\nCodename: noble\nArchitectures: amd64\n'
            'Components: main\n'
        )
        self.repository.joinpath('dists/noble/InRelease').write_text('signed\n')
        self.repository.joinpath('flussonix-archive-keyring.gpg').write_bytes(b'key')
        index.write_text(
            'Package: flussonix\nVersion: 0.10.0-1\nArchitecture: amd64\n\n'
            'Package: flussonix\nVersion: 0.11.0-1\nArchitecture: amd64\n\n'
        )
        self.rootfs = self.directory / 'qualification-root'
        self.result = self.directory / 'result.json'

    def options(self, **changes):
        module = load_runner()
        values = {
            'repository': self.repository,
            'public_url': None,
            'current_version': '0.11.0-1',
            'rootfs_dir': self.rootfs,
            'result_file': self.result,
            'keep_on_failure': False,
        }
        values.update(changes)
        return module, module.QualificationOptions(**values)

    def runner(self, adapter=None, **changes):
        module, options = self.options(**changes)
        removed = []
        runner = module.QualificationRunner(
            options,
            adapter=adapter or RecordingAdapter(),
            tool_lookup=lambda name: '/usr/bin/' + name,
            effective_uid=lambda: 0,
            remover=lambda path: removed.append(Path(path)),
            sleeper=lambda _seconds: None,
            token='a1b2c3d4',
        )
        return module, runner, removed

    def test_local_repository_requires_noble_amd64_and_both_versions(self):
        _module, runner, _removed = self.runner()
        runner.preflight()
        release = self.repository / 'dists/noble/Release'
        packages = self.repository / 'dists/noble/main/binary-amd64/Packages'
        for target, original, changed in (
            (release, release.read_text(), release.read_text().replace('noble', 'jammy')),
            (release, release.read_text(), release.read_text().replace('amd64', 'arm64')),
            (packages, packages.read_text(), packages.read_text().replace(
                'Version: 0.10.0-1', 'Version: 0.9.0-1')),
        ):
            with self.subTest(changed=changed):
                target.write_text(changed)
                with self.assertRaises(ValueError):
                    runner.preflight()
                target.write_text(original)

    def test_public_source_must_use_https(self):
        _module, runner, _removed = self.runner(
            repository=None, public_url='http://packages.invalid/apt'
        )
        with self.assertRaises(ValueError):
            runner.preflight()

    def test_preflight_requires_each_host_tool_and_result_outside_rootfs(self):
        module, options = self.options()
        runner = module.QualificationRunner(
            options,
            adapter=RecordingAdapter(),
            tool_lookup=lambda name: None if name == 'systemd-nspawn' else '/usr/bin/' + name,
            effective_uid=lambda: 0,
        )
        with self.assertRaisesRegex(RuntimeError, 'systemd-nspawn'):
            runner.preflight()

        _module, nested = self.options(result_file=self.rootfs / 'result.json')
        runner = module.QualificationRunner(
            nested,
            adapter=RecordingAdapter(),
            tool_lookup=lambda name: '/usr/bin/' + name,
            effective_uid=lambda: 0,
        )
        with self.assertRaisesRegex(ValueError, 'outside'):
            runner.preflight()

    def test_plan_uses_unique_private_network_machines_and_only_repository_bind(self):
        _module, runner, _removed = self.runner()
        operations = runner.plan()
        boots = [operation for operation in operations if operation.kind == 'boot']
        self.assertEqual(len(boots), 6)
        names = []
        for operation in boots:
            self.assertIn('--private-network', operation.argv)
            machine = next(value.split('=', 1)[1] for value in operation.argv
                           if value.startswith('--machine='))
            names.append(machine)
            self.assertIn('a1b2c3d4', machine)
            binds = [value for value in operation.argv if value.startswith('--bind')]
            self.assertEqual(len(binds), 1)
            source, destination = binds[0].split('=', 1)[1].split(':', 1)
            self.assertEqual(Path(source), self.repository.resolve())
            self.assertEqual(destination, '/srv/flussonix-repository')
        self.assertEqual(len(names), len(set(names)))

    def test_plan_bounds_every_wait_and_never_controls_host_service_or_reboot(self):
        _module, runner, _removed = self.runner()
        operations = runner.plan()
        self.assertTrue(operations)
        for operation in operations:
            self.assertGreater(operation.timeout, 0)
            self.assertLessEqual(operation.timeout, 1800)
            if operation.kind.startswith('wait_'):
                self.assertGreater(operation.retries, 0)
                self.assertLessEqual(operation.retries, 120)
            executable = Path(operation.argv[0]).name
            self.assertNotEqual(executable, 'reboot')
            self.assertNotEqual(executable, 'systemctl')
            if executable == 'systemd-run':
                self.assertIn('--expand-environment=no', operation.argv)
        host_commands = [
            ' '.join(operation.argv)
            for operation in operations
            if operation.kind != 'container_exec'
            and Path(operation.argv[0]).name != 'systemd-run'
        ]
        self.assertFalse(any('systemctl enable' in command for command in host_commands))

    def test_plan_covers_fresh_no_start_dependencies_and_upgrade_matrix(self):
        _module, runner, _removed = self.runner()
        operations = runner.plan()
        text = '\n'.join(operation.phase + ' ' + ' '.join(operation.argv)
                         for operation in operations)
        for value in (
            'signature-rejection', 'fresh-init', 'fresh-reboot',
            'no-start-init', 'ffmpeg', 'ffprobe', 'libsrt1.5-gnutls',
            'libsrtp2-1', 'adduser', 'dbus', '0.10.0-1', '0.11.0-1', 'rollback',
            'active-enabled', 'active-disabled',
            'inactive-enabled', 'inactive-disabled',
        ):
            self.assertIn(value, text)
        bootstrap = next(operation for operation in operations
                         if operation.phase == 'debootstrap-noble-amd64')
        self.assertIn('--components=main,universe', bootstrap.argv)

    def test_plan_bootstraps_tls_before_proving_signature_rejection(self):
        _module, runner, _removed = self.runner()
        operations = runner.plan()
        phases = [operation.phase for operation in operations]
        bootstrap = next(operation for operation in operations
                         if operation.phase == 'bootstrap-ca-certificates')
        rejection = next(operation for operation in operations
                         if operation.phase == 'signature-rejection')

        self.assertLess(phases.index('bootstrap-ca-certificates'),
                        phases.index('signature-rejection'))
        self.assertIn('ca-certificates', ' '.join(bootstrap.argv))
        self.assertIn('Dir::Etc::sourceparts=-', ' '.join(bootstrap.argv))
        self.assertFalse(rejection.expect_failure)
        rejection_command = ' '.join(rejection.argv)
        self.assertIn('NO_PUBKEY', rejection_command)
        self.assertIn('_Packages', rejection_command)

    def test_success_failure_and_interruption_cleanup_machines_and_rootfs(self):
        module, runner, removed = self.runner()
        probe = module.Operation('probe', ('/usr/bin/true',), 5)
        runner.execute([probe])
        self.assertEqual(removed, [self.rootfs])
        self.assertTrue(self.result.is_file())
        report = json.loads(self.result.read_text())
        self.assertEqual(report['status'], 'passed')
        self.assertEqual(report['cleanup']['machines_terminated'], True)

        for failure in ('probe', 'interrupt'):
            with self.subTest(failure=failure):
                self.result.unlink(missing_ok=True)
                adapter = RecordingAdapter(failure=failure)
                _module, failed, removed = self.runner(adapter=adapter)
                with self.assertRaises((RuntimeError, KeyboardInterrupt)):
                    failed.execute([probe])
                self.assertEqual(removed, [self.rootfs])
                self.assertTrue(any(Path(call.argv[0]).name == 'machinectl'
                                    and 'terminate' in call.argv for call in adapter.calls))
                report = json.loads(self.result.read_text())
                self.assertEqual(report['status'], 'failed')
                self.assertNotIn('secret', self.result.read_text().lower())

    def test_keep_on_failure_is_explicit_and_still_terminates_machines(self):
        module, runner, removed = self.runner(
            adapter=RecordingAdapter(failure='probe'), keep_on_failure=True
        )
        with self.assertRaises(RuntimeError):
            runner.execute([module.Operation('probe', ('/usr/bin/false',), 5)])
        self.assertEqual(removed, [])
        self.assertTrue(any('terminate' in call.argv for call in runner.adapter.calls))
        report = json.loads(self.result.read_text())
        self.assertEqual(report['cleanup']['rootfs_removed'], False)

    def test_cli_describes_keep_on_failure_as_diagnostic_only(self):
        result = subprocess.run(
            ['python3', str(SCRIPT), '--help'], capture_output=True, text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertRegex(result.stdout, r'(?i)keep-on-failure.*diagnostic')


if __name__ == '__main__':
    unittest.main()
