"""One-command APT bootstrap behavior with isolated host adapters."""
import importlib.util
import os
from pathlib import Path
import pty
import select
import subprocess
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
BUILDER = ROOT / 'scripts/build-apt-repository.py'
FINGERPRINT = '1234567890ABCDEF1234567890ABCDEF12345678'
REPOSITORY_URL = 'https://packages.example.invalid/FlussoniX/apt'


class InstallScriptTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='flussonix-install-test-')
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.root = self.directory / 'root'
        self.fake_bin = self.directory / 'bin'
        self.log = self.directory / 'commands.log'
        self.key = self.directory / 'archive-key.asc'
        self.root.joinpath('etc/ssl/certs').mkdir(parents=True)
        self.root.joinpath('etc/ssl/certs/ca-certificates.crt').write_text('fixture\n')
        self.root.joinpath('etc/os-release').write_text(
            'ID=ubuntu\nVERSION_ID="24.04"\n'
        )
        self.fake_bin.mkdir()
        self.key.write_text('public-key-fixture\n')
        self.write_adapter('dpkg', '''
if [ "$1" = "--print-architecture" ]; then
  printf '%s\\n' "${FAKE_ARCH:-amd64}"
  exit 0
fi
exit 2
''')
        self.write_adapter('apt-get', '''
printf 'apt-get' >>"$FAKE_LOG"
for argument in "$@"; do printf ' <%s>' "$argument" >>"$FAKE_LOG"; done
printf '\\n' >>"$FAKE_LOG"
if [ "${1:-}" = install ] && [ "${3:-}" = ca-certificates ]; then
  /bin/mkdir -p "$FAKE_ROOT/etc/ssl/certs"
  printf 'installed\\n' >"$FAKE_ROOT/etc/ssl/certs/ca-certificates.crt"
fi
''')
        self.write_adapter('curl', '''
if [ "${FAKE_CURL_FAIL:-0}" = 1 ]; then exit 22; fi
output=
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output" ] || [ "$1" = "-o" ]; then output=$2; shift 2; continue; fi
  shift
done
/bin/cp "$FAKE_PUBLIC_KEY" "$output"
''')
        self.write_adapter('gpg', '''
case " $* " in
  *" --show-keys "*)
    printf 'pub:-:255:22:ABC:0:0:::::scESC:::::ed25519:::0:\\n'
    printf 'fpr:::::::::%s:\\n' "${FAKE_FINGERPRINT:-$EXPECTED_TEST_FINGERPRINT}"
    ;;
  *" --dearmor "*)
    output=
    input=
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--output" ]; then output=$2; shift 2; continue; fi
      input=$1; shift
    done
    /bin/cp "$input" "$output"
    ;;
  *) exit 2 ;;
esac
''')
        self.write_adapter('install', '''
printf 'install' >>"$FAKE_LOG"
for argument in "$@"; do printf ' <%s>' "$argument" >>"$FAKE_LOG"; done
printf '\\n' >>"$FAKE_LOG"
if [ "$1" = "-d" ]; then
  shift
  if [ "$1" = "-m" ]; then mode=$2; shift 2; else mode=0755; fi
  /bin/mkdir -p "$1"; /bin/chmod "$mode" "$1"; exit 0
fi
if [ "$1" = "-m" ]; then mode=$2; source=$3; destination=$4; else exit 2; fi
/bin/mkdir -p "$(/usr/bin/dirname "$destination")"
/bin/cp "$source" "$destination"
/bin/chmod "$mode" "$destination"
''')
        self.script = self.directory / 'install.sh'
        module = self.load_builder()
        self.script.write_bytes(module.render_install_script(
            FINGERPRINT, repository_url=REPOSITORY_URL,
            root_prefix=str(self.root),
            command_path=str(self.fake_bin) + ':/usr/bin:/bin',
        ))
        self.script.chmod(0o755)

    def load_builder(self):
        spec = importlib.util.spec_from_file_location('flussonix_apt_builder', BUILDER)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def write_adapter(self, name, body):
        path = self.fake_bin / name
        path.write_text('#!/bin/sh\nset -eu\n' + body.lstrip())
        path.chmod(0o755)

    def environment(self, **changes):
        return {
            **os.environ,
            'PATH': str(self.fake_bin) + ':/usr/bin:/bin',
            'FAKE_LOG': str(self.log),
            'FAKE_PUBLIC_KEY': str(self.key),
            'FAKE_ROOT': str(self.root),
            'EXPECTED_TEST_FINGERPRINT': FINGERPRINT,
            **changes,
        }

    def run_script(self, **environment):
        return subprocess.run(
            ['/bin/sh', str(self.script)], env=self.environment(**environment),
            stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=20,
        )

    def run_interactive(self, **environment):
        pid, descriptor = pty.fork()
        if pid == 0:
            os.execve('/bin/sh', ['/bin/sh', str(self.script)],
                      self.environment(**environment))
        output = bytearray()
        deadline = time.monotonic() + 20
        status = None
        while time.monotonic() < deadline:
            ready, _, _ = select.select([descriptor], [], [], 0.1)
            if ready:
                try:
                    output.extend(os.read(descriptor, 4096))
                except OSError:
                    pass
            waited, status = os.waitpid(pid, os.WNOHANG)
            if waited == pid:
                break
        os.close(descriptor)
        if status is None:
            os.kill(pid, 9)
            os.waitpid(pid, 0)
            self.fail('interactive installer timed out')
        return os.waitstatus_to_exitcode(status), output.decode('utf-8', 'replace')

    def test_ubuntu_2404_amd64_writes_exact_scoped_source_and_is_idempotent(self):
        first = self.run_script()
        self.assertEqual(first.returncode, 0, first.stderr)
        source = self.root / 'etc/apt/sources.list.d/flussonix.sources'
        keyring = self.root / 'usr/share/keyrings/flussonix-archive-keyring.gpg'
        self.assertEqual(source.read_text(), (
            'Types: deb\n'
            f'URIs: {REPOSITORY_URL}\n'
            'Suites: noble\nComponents: main\nArchitectures: amd64\n'
            'Signed-By: /usr/share/keyrings/flussonix-archive-keyring.gpg\n'
        ))
        self.assertEqual(keyring.read_bytes(), self.key.read_bytes())
        self.assertEqual(source.stat().st_mode & 0o777, 0o644)
        self.assertEqual(keyring.stat().st_mode & 0o777, 0o644)
        self.assertIn('apt-get <update>', self.log.read_text())
        self.assertIn('apt-get <install> <-y> <flussonix>', self.log.read_text())
        self.assertIn('sudo flussonix --init', first.stdout)
        self.assertNotIn('PASSWORD', source.read_text() + first.stdout + first.stderr)

        second = self.run_script()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(source.read_text().count('Types: deb'), 1)

    def test_wrong_os_or_architecture_refuses_before_writes(self):
        self.root.joinpath('etc/os-release').write_text('ID=ubuntu\nVERSION_ID="22.04"\n')
        result = self.run_script()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.log.exists())
        self.assertFalse(self.root.joinpath('usr/share/keyrings').exists())

        self.root.joinpath('etc/os-release').write_text('ID=ubuntu\nVERSION_ID="24.04"\n')
        result = self.run_script(FAKE_ARCH='arm64')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.log.exists())
        self.assertFalse(self.root.joinpath('usr/share/keyrings').exists())

    def test_download_and_fingerprint_failure_leave_configuration_untouched(self):
        result = self.run_script(FAKE_CURL_FAIL='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.root.joinpath('etc/apt/sources.list.d').exists())
        self.assertFalse(self.log.exists())

        result = self.run_script(FAKE_FINGERPRINT='F' * 40)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.root.joinpath('etc/apt/sources.list.d').exists())
        self.assertFalse(self.log.exists())

    def test_missing_ca_bundle_installs_only_bootstrap_dependencies(self):
        self.root.joinpath('etc/ssl/certs/ca-certificates.crt').unlink()
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = self.log.read_text().splitlines()
        self.assertEqual(commands[0], 'apt-get <update>')
        self.assertEqual(
            commands[1],
            'apt-get <install> <-y> <ca-certificates> <gnupg>',
        )
        self.assertNotIn('curl', commands[1])

    def test_terminal_runs_wizard_and_initialized_host_skips_it(self):
        executable = self.root / 'usr/bin/flussonix'
        executable.parent.mkdir(parents=True)
        executable.write_text(
            '#!/bin/sh\nprintf "wizard" >>"$FAKE_LOG"\n'
            'for argument in "$@"; do printf " <%s>" "$argument" >>"$FAKE_LOG"; done\n'
            'printf "\\n" >>"$FAKE_LOG"\n'
        )
        executable.chmod(0o755)
        code, output = self.run_interactive()
        self.assertEqual(code, 0, output)
        self.assertIn('wizard <--init>', self.log.read_text())

        self.log.write_text('')
        credentials = self.root / 'etc/flussonix/credentials.env'
        credentials.parent.mkdir(parents=True, exist_ok=True)
        credentials.write_text('existing\n')
        code, output = self.run_interactive()
        self.assertEqual(code, 0, output)
        self.assertNotIn('wizard', self.log.read_text())


if __name__ == '__main__':
    unittest.main()
