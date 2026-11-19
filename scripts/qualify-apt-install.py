#!/usr/bin/env python3
"""Qualify the signed FlussoniX APT lifecycle in disposable Ubuntu machines.

The runner deliberately uses systemd-nspawn machines with private networking.
It never controls the host FlussoniX service, and removes every machine and
root filesystem unless the diagnostic-only keep option is selected after a
failure.
"""
import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import time
from typing import Optional
import urllib.parse
import urllib.request


SUITE = 'noble'
ARCHITECTURE = 'amd64'
PRIOR_VERSION = '0.10.0-1'
MACHINE_SCENARIOS = (
    'fresh-default',
    'fresh-no-start',
    'upgrade-active-enabled',
    'upgrade-active-disabled',
    'upgrade-inactive-enabled',
    'upgrade-inactive-disabled',
)
REQUIRED_TOOLS = (
    'cp',
    'debootstrap',
    'systemd-nspawn',
    'machinectl',
    'systemd-run',
)
MAX_METADATA = 16 * 1024 * 1024


@dataclass(frozen=True)
class QualificationOptions:
    repository: Optional[Path]
    public_url: Optional[str]
    current_version: str
    rootfs_dir: Path
    result_file: Path
    keep_on_failure: bool = False


@dataclass(frozen=True)
class Operation:
    phase: str
    argv: tuple
    timeout: int
    kind: str = 'command'
    retries: int = 1
    expect_failure: bool = False
    fact: Optional[str] = None
    input_text: Optional[str] = None


class ProcessAdapter:
    """Bounded subprocess adapter with asynchronous nspawn boot ownership."""

    def __init__(self):
        self.processes = []

    def run(self, operation):
        if operation.kind == 'boot':
            process = subprocess.Popen(
                operation.argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            self.processes.append(process)
            return subprocess.CompletedProcess(operation.argv, 0, '', '')
        standard_input = (
            {'stdin': subprocess.DEVNULL}
            if operation.input_text is None
            else {'input': operation.input_text}
        )
        return subprocess.run(
            operation.argv,
            capture_output=True,
            text=True,
            timeout=operation.timeout,
            check=False,
            **standard_input,
        )

    def close(self):
        for process in self.processes:
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


class QualificationRunner:
    def __init__(
        self,
        options,
        adapter=None,
        tool_lookup=shutil.which,
        effective_uid=os.geteuid,
        remover=shutil.rmtree,
        sleeper=time.sleep,
        token=None,
    ):
        self.options = options
        self.adapter = adapter or ProcessAdapter()
        self.tool_lookup = tool_lookup
        self.effective_uid = effective_uid
        self.remover = remover
        self.sleeper = sleeper
        self.token = token or secrets.token_hex(4)
        self.tools = {}
        self.machine_names = [f'flussonix-apt-{self.token}-{name}' for name in MACHINE_SCENARIOS]
        self.machine_roots = {
            name: self.options.rootfs_dir / name for name in MACHINE_SCENARIOS
        }
        self.base_root = self.options.rootfs_dir / 'base-noble-amd64'
        self.completed_phases = []
        self.facts = {}
        self._admin_secret = secrets.token_urlsafe(30)
        self._peer_secret = secrets.token_hex(32)

    def _tool(self, name):
        return self.tools.get(name) or self.tool_lookup(name) or name

    @staticmethod
    def _read_bounded(path):
        path = Path(path)
        metadata = path.stat()
        if not path.is_file() or metadata.st_size > MAX_METADATA:
            raise ValueError(f'invalid bounded repository metadata: {path.name}')
        return path.read_text(encoding='utf-8')

    @staticmethod
    def _validate_release(release):
        fields = {}
        for line in release.splitlines():
            if ': ' in line:
                key, value = line.split(': ', 1)
                fields[key] = value
        if fields.get('Codename') != SUITE:
            raise ValueError('repository suite must be Ubuntu noble')
        if ARCHITECTURE not in fields.get('Architectures', '').split():
            raise ValueError('repository must publish amd64')
        if 'main' not in fields.get('Components', '').split():
            raise ValueError('repository must publish the main component')

    def _validate_packages(self, packages):
        versions = set(re.findall(
            r'(?ms)^Package: flussonix\n(?:[^\n]+\n)*?Version: ([^\n]+)$', packages
        ))
        required = {PRIOR_VERSION, self.options.current_version}
        if not required <= versions:
            missing = ', '.join(sorted(required - versions))
            raise ValueError(f'repository is missing required FlussoniX versions: {missing}')

    def _validate_local_repository(self, repository):
        repository = repository.resolve()
        release = repository / 'dists/noble/Release'
        inrelease = repository / 'dists/noble/InRelease'
        packages = repository / 'dists/noble/main/binary-amd64/Packages'
        key = repository / 'flussonix-archive-keyring.gpg'
        for path in (release, inrelease, packages, key):
            if path.is_symlink() or not path.is_file():
                raise ValueError(f'local repository is missing {path.name}')
        self._validate_release(self._read_bounded(release))
        self._validate_packages(self._read_bounded(packages))

    def _validate_public_repository(self, url):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != 'https' or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError('public repository URL must be an HTTPS origin without credentials')
        if parsed.query or parsed.fragment:
            raise ValueError('public repository URL must not contain a query or fragment')

    def preflight(self):
        if self.effective_uid() != 0:
            raise PermissionError('APT qualification requires root')
        if bool(self.options.repository) == bool(self.options.public_url):
            raise ValueError('select exactly one repository source')
        if not re.fullmatch(r'[0-9A-Za-z.+:~-]+', self.options.current_version):
            raise ValueError('invalid current package version')
        if self.options.current_version == PRIOR_VERSION:
            raise ValueError('current version must differ from the prior qualified version')
        rootfs = self.options.rootfs_dir.resolve()
        result = self.options.result_file.resolve()
        if rootfs == Path('/') or result == Path('/') or rootfs in result.parents:
            raise ValueError('result file must be outside the disposable rootfs')
        if self.options.rootfs_dir.exists():
            raise FileExistsError('rootfs directory already exists')
        for name in REQUIRED_TOOLS:
            path = self.tool_lookup(name)
            if not path:
                raise RuntimeError(f'required host tool is unavailable: {name}')
            self.tools[name] = path
        if self.options.repository:
            self._validate_local_repository(Path(self.options.repository))
        else:
            self._validate_public_repository(self.options.public_url)

    def _repo_bind(self):
        if not self.options.repository:
            return ()
        source = Path(self.options.repository).resolve()
        return (f'--bind-ro={source}:/srv/flussonix-repository',)

    def _nspawn(self, root, *command):
        return (
            self._tool('systemd-nspawn'), '--quiet', f'--directory={root}',
            *self._repo_bind(), '--', *command,
        )

    def _machine_command(self, machine, command):
        return (
            self._tool('systemd-run'), f'--machine={machine}', '--pipe', '--wait',
            '--quiet', '--collect', '--expand-environment=no', '/bin/bash', '-ec', command,
        )

    def _boot(self, scenario, machine):
        argv = [
            self._tool('systemd-nspawn'), '--quiet', '--boot', '--register=yes',
            f'--machine={machine}', f'--directory={self.machine_roots[scenario]}',
            '--private-network',
        ]
        argv.extend(self._repo_bind())
        return Operation(f'{scenario}-boot', tuple(argv), 120, 'boot')

    def _wait(self, phase, machine, state='running', retries=120):
        return Operation(
            phase,
            (self._tool('machinectl'), 'show', machine, '--property=State', '--value'),
            5,
            f'wait_{state}',
            retries,
        )

    def _terminate(self, phase, machine):
        return Operation(
            phase, (self._tool('machinectl'), 'terminate', machine), 30, 'terminate'
        )

    def _exec(self, phase, machine, command, timeout=300, fact=None):
        return Operation(
            phase, self._machine_command(machine, command), timeout,
            'container_exec', 1, False, fact,
        )

    def _wait_exec(self, phase, machine, command, retries=120):
        return Operation(
            phase,
            self._machine_command(machine, command),
            5,
            'wait_ready',
            retries,
        )

    def _seed_init_operation(self, scenario, machine):
        content = (
            'FLUSSONIX_ADMIN_USER=admin\n'
            f'FLUSSONIX_ADMIN_PASSWORD={self._admin_secret}\n'
            f'FLUSSONIX_PEER_KEY={self._peer_secret}\n'
        )
        return Operation(
            f'{scenario}-seed-init',
            self._machine_command(
                machine,
                "umask 077; cat > /run/flussonix-qualifier.env; "
                "chown root:root /run/flussonix-qualifier.env; "
                "chmod 0600 /run/flussonix-qualifier.env",
            ),
            30,
            'container_exec',
            1,
            False,
            None,
            content,
        )

    def _install_command(self, version):
        escaped = version.replace("'", "'\\''")
        return (
            "export DEBIAN_FRONTEND=noninteractive; "
            "cp /opt/flussonix-qualifier-cache/flussonix_*_amd64.deb "
            "/var/cache/apt/archives/; "
            f"apt-get install -y --no-download --no-install-recommends 'flussonix={escaped}'"
        )

    def _upgrade_verify_command(self, version, active, enabled, phase):
        active_check = 'systemctl is-active --quiet flussonix.service' if active else (
            '! systemctl is-active --quiet flussonix.service'
        )
        enabled_check = 'systemctl is-enabled --quiet flussonix.service' if enabled else (
            '! systemctl is-enabled --quiet flussonix.service'
        )
        health = "curl -fsS http://127.0.0.1:18210/health >/dev/null" if active else 'true'
        return (
            f"test \"$(dpkg-query -W -f='${{Version}}' flussonix)\" = '{version}'; "
            f"{active_check}; {enabled_check}; {health}; "
            "test \"$(cat /var/lib/flussonix/qualification-marker)\" = preserved; "
            "test -s /var/lib/flussonix/config.json; "
            "test -s /var/lib/flussonix/config.json.node-id; "
            "test -s /var/lib/flussonix/iptv.json; "
            f"printf '%s\\n' '{phase}'"
        )

    def plan(self):
        operations = [
            Operation(
                'debootstrap-noble-amd64',
                (self._tool('debootstrap'), '--arch=amd64', '--variant=minbase',
                 '--components=main,universe', 'noble', str(self.base_root),
                 'http://us.archive.ubuntu.com/ubuntu'),
                1800,
            ),
            Operation('configure-signed-repository', ('internal', 'configure-base'), 30, 'internal'),
            Operation(
                'signature-rejection',
                self._nspawn(
                    self.base_root, '/bin/bash', '-ec',
                    "mkdir -p /var/lib/apt/lists-invalid/partial; "
                    "printf 'invalid-key' >/tmp/invalid.gpg; "
                    "sed 's#Signed-By: .*#Signed-By: /tmp/invalid.gpg#' "
                    "/etc/apt/sources.list.d/flussonix.sources >/tmp/invalid.sources; "
                    "apt-get update -o Dir::Etc::sourcelist=/tmp/invalid.sources "
                    "-o Dir::Etc::sourceparts=- -o Dir::State::Lists=/var/lib/apt/lists-invalid"
                ),
                300,
                expect_failure=True,
            ),
            Operation(
                'base-dependencies-ffmpeg-ffprobe-libsrt1.5-gnutls-libsrtp2-1-adduser-dbus',
                self._nspawn(
                    self.base_root, '/bin/bash', '-ec',
                    "export DEBIAN_FRONTEND=noninteractive; apt-get update; "
                    "apt-get install -y --no-install-recommends "
                    "ffmpeg libsrt1.5-gnutls libsrtp2-1 ca-certificates adduser curl dbus systemd"
                ),
                1800,
            ),
            Operation(
                f'cache-packages-{PRIOR_VERSION}-{self.options.current_version}',
                self._nspawn(
                    self.base_root, '/bin/bash', '-ec',
                    "mkdir -p /opt/flussonix-qualifier-cache; "
                    "cd /opt/flussonix-qualifier-cache; "
                    f"apt-get download flussonix={PRIOR_VERSION}; "
                    f"apt-get download flussonix={self.options.current_version}; "
                    "test \"$(find . -maxdepth 1 -name 'flussonix_*_amd64.deb' | wc -l)\" -eq 2"
                ),
                600,
            ),
        ]

        for scenario, machine in zip(MACHINE_SCENARIOS, self.machine_names):
            operations.append(Operation(
                f'{scenario}-clone', ('internal', 'clone', scenario), 600, 'internal'
            ))
            version = self.options.current_version if scenario.startswith('fresh-') else PRIOR_VERSION
            operations.append(Operation(
                f'{scenario}-install-{version}',
                self._nspawn(
                    self.machine_roots[scenario], '/bin/bash', '-ec',
                    self._install_command(version),
                ),
                900,
            ))
            if scenario.startswith('upgrade-'):
                operations.append(Operation(
                    f'{scenario}-seed-state', ('internal', 'seed-upgrade', scenario),
                    30, 'internal'
                ))
            operations.extend((
                self._boot(scenario, machine),
                self._wait(f'{scenario}-wait-running', machine),
                Operation(
                    f'{scenario}-wait-management-ready',
                    (self._tool('systemd-run'), f'--machine={machine}', '--pipe', '--wait',
                     '--quiet', '--collect', '--expand-environment=no', '/bin/true'),
                    5,
                    'wait_ready',
                    120,
                ),
            ))
            if scenario.startswith('fresh-'):
                operations.append(self._seed_init_operation(scenario, machine))

            if scenario == 'fresh-default':
                operations.extend((
                    self._exec(
                        'fresh-init', machine,
                        "set -a; . /run/flussonix-qualifier.env; set +a; "
                        "rm -f /run/flussonix-qualifier.env; "
                        "flussonix --init --non-interactive --role standalone "
                        "--node-name apt-fresh --listen 127.0.0.1:18210",
                        300,
                    ),
                    self._exec(
                        'fresh-dependencies-and-health', machine,
                        "systemctl is-enabled --quiet flussonix.service; "
                        "systemctl is-active --quiet flussonix.service; "
                        "command -v ffmpeg; command -v ffprobe; "
                        "dpkg-query -W -f='${Version}\\n' libsrt1.5-gnutls libsrtp2-1; "
                        "curl -fsS http://127.0.0.1:18210/health >/dev/null",
                        120,
                        'fresh_dependencies',
                    ),
                    Operation(
                        'fresh-reboot',
                        (self._tool('machinectl'), 'reboot', machine),
                        30,
                        'reboot',
                    ),
                    self._wait('fresh-reboot-wait-running', machine),
                    Operation(
                        'fresh-reboot-health',
                        self._machine_command(
                            machine,
                            "systemctl is-active --quiet flussonix.service; "
                            "curl -fsS http://127.0.0.1:18210/health >/dev/null",
                        ),
                        5,
                        'wait_ready',
                        120,
                    ),
                    self._exec(
                        'fresh-remove', machine,
                        "export DEBIAN_FRONTEND=noninteractive; apt-get remove -y flussonix; "
                        "! dpkg-query -W -f='${db:Status-Status}\\n' flussonix "
                        "2>/dev/null | grep -qx installed; "
                        "test -f /etc/flussonix/credentials.env; "
                        "test -d /var/lib/flussonix",
                        300,
                    ),
                    self._exec(
                        'fresh-reinstall-before-purge', machine,
                        self._install_command(self.options.current_version),
                        600,
                    ),
                    self._exec(
                        'fresh-purge', machine,
                        "export DEBIAN_FRONTEND=noninteractive; apt-get purge -y flussonix; "
                        "test ! -e /etc/flussonix/credentials.env; "
                        "test ! -e /etc/systemd/system/flussonix.service.d/10-setup.conf; "
                        "test -d /var/lib/flussonix",
                        300,
                    ),
                ))
            elif scenario == 'fresh-no-start':
                operations.extend((
                    self._exec(
                        'no-start-init', machine,
                        "set -a; . /run/flussonix-qualifier.env; set +a; "
                        "rm -f /run/flussonix-qualifier.env; "
                        "flussonix --init --non-interactive --no-start --role standalone "
                        "--node-name apt-no-start --listen 127.0.0.1:18210",
                        300,
                    ),
                    self._exec(
                        'no-start-verify', machine,
                        "! systemctl is-active --quiet flussonix.service; "
                        "! systemctl is-enabled --quiet flussonix.service; "
                        "test -s /etc/flussonix/credentials.env; "
                        "command -v ffmpeg; command -v ffprobe; "
                        "dpkg-query -W libsrt1.5-gnutls libsrtp2-1 >/dev/null",
                        120,
                    ),
                ))
            else:
                active = '-active-' in scenario and '-inactive-' not in scenario
                enabled = scenario.endswith('-enabled')
                set_active = 'systemctl start flussonix.service' if active else (
                    'systemctl stop flussonix.service'
                )
                set_enabled = 'systemctl enable flussonix.service' if enabled else (
                    'systemctl disable flussonix.service'
                )
                operations.extend((
                    self._wait_exec(
                        f'{scenario}-establish', machine,
                        f"{set_enabled}; {set_active}; "
                        + self._upgrade_verify_command(PRIOR_VERSION, active, enabled, 'legacy'),
                    ),
                    self._exec(
                        f'{scenario}-upgrade-{self.options.current_version}', machine,
                        self._install_command(self.options.current_version),
                        600,
                    ),
                    self._wait_exec(
                        f'{scenario}-verify-upgrade', machine,
                        self._upgrade_verify_command(
                            self.options.current_version, active, enabled, 'upgraded'
                        ),
                    ),
                    self._exec(
                        f'{scenario}-rollback-{PRIOR_VERSION}', machine,
                        "export DEBIAN_FRONTEND=noninteractive; "
                        "cp /opt/flussonix-qualifier-cache/flussonix_*_amd64.deb "
                        "/var/cache/apt/archives/; "
                        f"apt-get install -y --allow-downgrades --no-download "
                        f"--no-install-recommends flussonix={PRIOR_VERSION}",
                        600,
                    ),
                    self._wait_exec(
                        f'{scenario}-verify-rollback', machine,
                        self._upgrade_verify_command(PRIOR_VERSION, active, enabled, 'rollback'),
                    ),
                ))
            operations.append(self._terminate(f'{scenario}-terminate', machine))
        return operations

    @staticmethod
    def _passwd_uid(root, account):
        for line in (root / 'etc/passwd').read_text().splitlines():
            fields = line.split(':')
            if fields[0] == account:
                return int(fields[2]), int(fields[3])
        raise RuntimeError(f'account is absent in qualification root: {account}')

    @staticmethod
    def _write_private(path, content, mode=0o600, uid=0, gid=0):
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            raise RuntimeError('refusing qualification symlink')
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, mode)
        try:
            os.write(descriptor, content)
            os.fchmod(descriptor, mode)
            os.fchown(descriptor, uid, gid)
        finally:
            os.close(descriptor)

    def _configure_base(self):
        key_destination = self.base_root / 'usr/share/keyrings/flussonix-archive-keyring.gpg'
        key_destination.parent.mkdir(parents=True, exist_ok=True)
        if self.options.repository:
            shutil.copyfile(
                Path(self.options.repository) / 'flussonix-archive-keyring.gpg', key_destination
            )
            uri = 'file:/srv/flussonix-repository'
        else:
            with urllib.request.urlopen(
                self.options.public_url.rstrip('/') + '/flussonix-archive-keyring.gpg', timeout=30
            ) as response:
                data = response.read(MAX_METADATA + 1)
            if len(data) > MAX_METADATA:
                raise ValueError('public repository key exceeds size limit')
            key_destination.write_bytes(data)
            uri = self.options.public_url.rstrip('/')
        os.chmod(key_destination, 0o644)
        source = (
            'Types: deb\n'
            f'URIs: {uri}\n'
            'Suites: noble\n'
            'Components: main\n'
            'Architectures: amd64\n'
            'Signed-By: /usr/share/keyrings/flussonix-archive-keyring.gpg\n'
        )
        sources = self.base_root / 'etc/apt/sources.list.d/flussonix.sources'
        sources.parent.mkdir(parents=True, exist_ok=True)
        sources.write_text(source)
        os.chmod(sources, 0o644)

    def _clone(self, scenario):
        target = self.machine_roots[scenario]
        if target.exists():
            raise FileExistsError(target)
        result = subprocess.run(
            (self._tool('cp'), '--archive', '--reflink=auto', str(self.base_root), str(target)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=600,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError('bounded rootfs clone failed')
        # systemd-nspawn used the base for package preparation. Every booted
        # clone must generate an identity from its unique container identity.
        machine_id = target / 'etc/machine-id'
        if machine_id.is_symlink() or not machine_id.is_file():
            raise RuntimeError('qualification machine-id is unsafe')
        machine_id.write_text('')

    def _seed_init(self, scenario):
        root = self.machine_roots[scenario]
        content = (
            f'FLUSSONIX_ADMIN_USER=admin\n'
            f'FLUSSONIX_ADMIN_PASSWORD={self._admin_secret}\n'
            f'FLUSSONIX_PEER_KEY={self._peer_secret}\n'
        ).encode()
        self._write_private(root / 'run/flussonix-qualifier.env', content)

    def _seed_upgrade(self, scenario):
        root = self.machine_roots[scenario]
        uid, gid = self._passwd_uid(root, 'flussonix')
        credentials = (
            f'FLUSSONIX_ADMIN_PASSWORD={self._admin_secret}\n'
            f'FLUSSONIX_PEER_KEY={self._peer_secret}\n'
        ).encode()
        self._write_private(root / 'etc/flussonix/credentials.env', credentials)
        drop_in = (
            '[Service]\nEnvironmentFile=/etc/flussonix/credentials.env\n'
            'ExecStart=\n'
            'ExecStart=/usr/bin/flussonix --listen=127.0.0.1:18210 '
            '--config=/var/lib/flussonix/config.json '
            '--media-dir=/var/lib/flussonix/media '
            '--web-dir=/usr/share/flussonix/web --admin-user=admin '
            '--role=standalone --node-name=apt-upgrade\n'
        ).encode()
        self._write_private(
            root / 'etc/systemd/system/flussonix.service.d/10-setup.conf',
            drop_in, 0o644,
        )
        state = root / 'var/lib/flussonix'
        media = state / 'media'
        media.mkdir(parents=True, exist_ok=True)
        os.chown(media, uid, gid)
        os.chmod(media, 0o700)
        documents = {
            'config.json': (
                '{"streams":[],"templates":[],"peers":[],"sources":[],'
                '"auth_backends":[],"balancers":[]}\n'
            ),
            'config.json.node-id': '2f9dbe22-62db-4d29-83bb-10a6b995f23d\n',
            'iptv.json': '{"version":1,"enabled":false,"packages":[],"subscribers":[]}\n',
            'qualification-marker': 'preserved\n',
        }
        for name, content in documents.items():
            self._write_private(state / name, content.encode(), 0o600, uid, gid)

    def _run_internal(self, operation):
        action = operation.argv[1]
        if action == 'configure-base':
            self._configure_base()
        elif action == 'clone':
            self._clone(operation.argv[2])
        elif action == 'seed-init':
            self._seed_init(operation.argv[2])
        elif action == 'seed-upgrade':
            self._seed_upgrade(operation.argv[2])
        else:
            raise RuntimeError(f'unknown qualification internal action: {action}')
        return subprocess.CompletedProcess(operation.argv, 0, '', '')

    def _run_wait(self, operation):
        expected = operation.kind.removeprefix('wait_')
        last = None
        for attempt in range(operation.retries):
            try:
                last = self.adapter.run(operation)
            except subprocess.TimeoutExpired:
                last = subprocess.CompletedProcess(operation.argv, 124, '', '')
            state = (last.stdout or '').strip()
            if last.returncode == 0 and (expected == 'ready' or state == expected):
                return last
            if attempt + 1 < operation.retries:
                self.sleeper(0.5)
        raise RuntimeError(f'{operation.phase} exceeded its bounded wait')

    def _run_operation(self, operation):
        if operation.kind == 'internal':
            result = self._run_internal(operation)
        elif operation.kind.startswith('wait_'):
            result = self._run_wait(operation)
        else:
            result = self.adapter.run(operation)
        if operation.expect_failure:
            if result.returncode == 0:
                raise RuntimeError(f'{operation.phase} unexpectedly succeeded')
        elif result.returncode != 0:
            raise RuntimeError(f'{operation.phase} failed with status {result.returncode}')
        if operation.fact:
            value = (result.stdout or '').strip()
            self.facts[operation.fact] = value[:4096]
        self.completed_phases.append(operation.phase)

    def _cleanup(self, failed):
        for machine in self.machine_names:
            try:
                self.adapter.run(Operation(
                    f'cleanup-{machine}',
                    (self._tool('machinectl'), 'terminate', machine),
                    30,
                    'cleanup',
                ))
            except BaseException:
                pass
        close = getattr(self.adapter, 'close', None)
        if close:
            try:
                close()
            except BaseException:
                pass
        alive = set(self.machine_names)
        for attempt in range(40):
            remaining = set()
            for machine in alive:
                try:
                    result = self.adapter.run(Operation(
                        f'cleanup-check-{machine}',
                        (self._tool('machinectl'), 'show', machine),
                        5,
                        'cleanup',
                    ))
                    if result.returncode == 0:
                        remaining.add(machine)
                except BaseException:
                    remaining.add(machine)
            alive = remaining
            if not alive:
                break
            if attempt + 1 < 40:
                self.sleeper(0.25)
        remove_root = not (failed and self.options.keep_on_failure)
        rootfs_removed = False
        if remove_root:
            try:
                self.remover(self.options.rootfs_dir)
            except FileNotFoundError:
                rootfs_removed = True
            except OSError:
                rootfs_removed = False
            else:
                rootfs_removed = True
        return {'machines_terminated': not alive, 'rootfs_removed': rootfs_removed}

    def _write_result(self, status, cleanup, failed_phase=None):
        report = {
            'format': 1,
            'status': status,
            'suite': SUITE,
            'architecture': ARCHITECTURE,
            'versions': {'prior': PRIOR_VERSION, 'current': self.options.current_version},
            'phases': list(self.completed_phases),
            'facts': self.facts,
            'cleanup': cleanup,
        }
        if failed_phase:
            report['failed_phase'] = failed_phase
        self.options.result_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.options.result_file.with_name(self.options.result_file.name + '.tmp')
        with open(temporary, 'w', encoding='utf-8') as result:
            json.dump(report, result, sort_keys=True, indent=2)
            result.write('\n')
            result.flush()
            os.fsync(result.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.options.result_file)

    def execute(self, operations):
        failed = True
        failed_phase = None
        error = None
        try:
            for operation in operations:
                failed_phase = operation.phase
                self._run_operation(operation)
            failed = False
        except BaseException as caught:
            error = caught
        cleanup = self._cleanup(failed)
        cleanup_complete = cleanup['machines_terminated'] and (
            cleanup['rootfs_removed'] or (failed and self.options.keep_on_failure)
        )
        if not cleanup_complete and error is None:
            failed = True
            failed_phase = 'cleanup'
            error = RuntimeError('qualification cleanup did not complete')
        self._write_result('failed' if failed else 'passed', cleanup, failed_phase if failed else None)
        if error is not None:
            raise error
        return self.options.result_file


def parser():
    command = argparse.ArgumentParser(
        description='Qualify fresh install, upgrade, rollback and removal in disposable noble machines.'
    )
    source = command.add_mutually_exclusive_group(required=True)
    source.add_argument('--repository', type=Path, help='local signed APT repository')
    source.add_argument('--public-url', help='published HTTPS APT repository root')
    command.add_argument('--current-version', required=True, help='current Debian version, e.g. 0.11.0-1')
    command.add_argument('--rootfs-dir', required=True, type=Path, help='new disposable root directory')
    command.add_argument('--result-file', type=Path, help='owner-only JSON result outside the rootfs')
    command.add_argument(
        '--keep-on-failure', action='store_true',
        help='keep rootfs on failure for diagnostic use only; machines are always terminated',
    )
    return command


def main(argv=None):
    arguments = parser().parse_args(argv)
    result_file = arguments.result_file or arguments.rootfs_dir.with_name(
        arguments.rootfs_dir.name + '-result.json'
    )
    options = QualificationOptions(
        repository=arguments.repository,
        public_url=arguments.public_url,
        current_version=arguments.current_version,
        rootfs_dir=arguments.rootfs_dir,
        result_file=result_file,
        keep_on_failure=arguments.keep_on_failure,
    )
    runner = QualificationRunner(options)
    try:
        runner.preflight()
        runner.execute(runner.plan())
    except KeyboardInterrupt:
        print(f'qualification interrupted; result: {result_file}', file=sys.stderr)
        return 130
    except Exception as error:
        print(f'qualification failed ({type(error).__name__}); result: {result_file}', file=sys.stderr)
        return 1
    print(f'qualification passed; result: {result_file}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
