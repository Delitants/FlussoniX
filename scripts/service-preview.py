#!/usr/bin/env python3
"""Create credentials and render an optional systemd preview unit (Python 3.11+)."""
import argparse
import getpass
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import pwd
import re
import stat


def absolute(raw):
    path = Path(raw)
    resolved = path.resolve()
    if any(not re.fullmatch(r'/[A-Za-z0-9_./:@+-]+', str(p)) for p in [path, resolved]) or path.is_symlink():
        raise ValueError('use absolute, non-linked paths without spaces or substitution characters')
    return resolved


def read_regular(path, limit):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC), 'rb') as f:
        info = os.fstat(f.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ValueError('input must be a bounded regular file')
        data = f.read(limit + 1)
        if len(data) > limit:
            raise ValueError('input exceeds size limit')
        return data, info


def create_file(path, data, mode):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, mode)
    identity = os.fstat(fd)
    try:
        with os.fdopen(fd, 'wb') as output:
            os.fchmod(output.fileno(), mode)
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
    except BaseException:
        try:
            current = path.stat(follow_symlinks=False)
            if (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino):
                path.unlink()
        except FileNotFoundError:
            pass
        raise


def credentials(args):
    if args.from_env:
        user = os.environ.get('FLUSSONIX_ADMIN_USER', 'admin')
        password = os.environ.get('FLUSSONIX_ADMIN_PASSWORD', '')
        peer = os.environ.get('FLUSSONIX_PEER_KEY', '')
    else:
        user = args.admin_user
        password = getpass.getpass('Saved admin password: ')
        peer = getpass.getpass('Saved cluster peer key: ')
    values = {'FLUSSONIX_ADMIN_USER': user, 'FLUSSONIX_ADMIN_PASSWORD': password, 'FLUSSONIX_PEER_KEY': peer}
    if any(not value or len(value) > 4096 or any(not 32 <= ord(c) <= 126 for c in value)
           for value in values.values()) or len(peer) < 12 or ':' in user:
        raise ValueError('credentials require printable ASCII; username cannot contain a colon; peer key needs at least 12 characters')
    # EnvironmentFile double quotes preserve these characters when backslash escaped.
    def quote(value):
        for character in ['\\', '"', '`', '$']:
            value = value.replace(character, '\\' + character)
        return '"' + value + '"'
    data = ''.join(f'{key}={quote(value)}\n' for key, value in values.items()).encode()
    path = absolute(args.output)
    create_file(path, data, 0o600)
    print(f'Credentials saved to {path}')


def can_access(path, account, groups, bits):
    info = path.stat()
    shift = 6 if info.st_uid == account.pw_uid else 3 if info.st_gid in groups else 0
    return (info.st_mode >> shift) & bits == bits


def traverse(path, account, groups):
    for parent in [path, *path.parents]:
        if not parent.is_dir() or not can_access(parent, account, groups, 1):
            raise ValueError('service account cannot traverse a selected directory')


def verified_release(release, account, groups):
    raw, _ = read_regular(release / 'manifest.json', 1024 * 1024)
    manifest = json.loads(raw)
    if not isinstance(manifest, dict) or manifest.get('format') != 1 or manifest.get('name') != 'FlussoniX':
        raise ValueError('release requires a FlussoniX package manifest')
    files = manifest.get('files')
    if not isinstance(files, dict) or not {'bin/flussonix', 'web/index.html'} <= files.keys() or len(files) > 2048:
        raise ValueError('release manifest is incomplete or exceeds limits')
    total = 0
    for name, row in files.items():
        if not isinstance(row, dict) or type(row.get('bytes')) is not int or not isinstance(row.get('sha256'), str):
            raise ValueError('invalid manifest payload metadata')
        path = Path(name)
        if path.is_absolute() or not path.parts or any(not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]*', p) for p in path.parts):
            raise ValueError('invalid manifest payload path')
        full = release / path
        if any((release / Path(*path.parts[:i])).is_symlink() for i in range(1, len(path.parts) + 1)):
            raise ValueError('release payload cannot contain links')
        data, _ = read_regular(full, 128 * 1024 * 1024)
        total += len(data)
        if total > 256 * 1024 * 1024 or len(data) != row['bytes'] or hashlib.sha256(data).hexdigest() != row['sha256']:
            raise ValueError('release payload does not match its manifest')
        traverse(full.parent, account, groups)
        if not can_access(full, account, groups, 5 if name == 'bin/flussonix' else 4):
            raise ValueError('service account cannot read or execute the release')
    expected = set()
    for name in files:
        path = Path(name)
        if path.parts[0] == 'web':
            expected.add(path)
            expected.update(parent for parent in path.parents if parent != Path('.'))
    actual = {Path('web')}
    for path in (release / 'web').rglob('*'):
        if path.is_symlink() or len(actual) >= 2049 or not (path.is_file() or path.is_dir()):
            raise ValueError('public web tree cannot contain links or unexpected entries')
        actual.add(path.relative_to(release))
    if actual != expected:
        raise ValueError('public web tree must match the release manifest exactly')


def listener(value):
    match = re.fullmatch(r'(\[[0-9A-Fa-f:]+\]|[0-9.]+):([0-9]+)', value)
    if not match:
        raise ValueError('listener must be a literal IP address and port')
    host, port = match.groups()
    address = ipaddress.ip_address(host.strip('[]'))
    if host.startswith('[') != (address.version == 6):
        raise ValueError('use brackets only around IPv6 addresses')
    port = int(port)
    if address.is_multicast or not (port == 0 or 1024 <= port <= 65535):
        raise ValueError('listener requires a unicast/wildcard IP and port 0 or 1024..65535')
    return f'[{address}]:{port}' if address.version == 6 else f'{address}:{port}'


def render(args):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,39}', args.instance):
        raise ValueError('instance must contain lowercase letters, digits and hyphens')
    account = pwd.getpwnam(args.user)
    if account.pw_uid == 0:
        raise ValueError('choose a non-root service account')
    groups = os.getgrouplist(account.pw_name, account.pw_gid)
    release, state, secret, ffmpeg = map(absolute, [args.release_dir, args.state_dir, args.credentials, args.ffmpeg])
    if state.is_relative_to(release) or release.is_relative_to(state) or secret.is_relative_to(release):
        raise ValueError('state and credentials must be separate from the release')
    if not state.is_dir() or state.stat().st_uid != account.pw_uid or state.stat().st_mode & 0o077:
        raise ValueError('state directory must belong to the service account with mode 0700')
    traverse(state, account, groups)
    if not can_access(state, account, groups, 7):
        raise ValueError('service account needs read/write access to state')
    config = state / 'config.json'
    if config.exists() or config.is_symlink():
        _, info = read_regular(config, 16 * 1024 * 1024)
        if info.st_uid != account.pw_uid or info.st_mode & 0o077 or not can_access(config, account, groups, 6):
            raise ValueError('existing config must be private and readable/writable by the service account')
    data, info = read_regular(secret, 32 * 1024)
    if info.st_uid not in [0, account.pw_uid] or info.st_mode & 0o077:
        raise ValueError('credentials must be owner-only and belong to root or the service account')
    if not data:
        raise ValueError('credentials file is empty')
    verified_release(release, account, groups)
    if not ffmpeg.is_file() or not can_access(ffmpeg, account, groups, 5) or ffmpeg.is_relative_to('/opt/flussonic'):
        raise ValueError('select an independently installed readable/executable FFmpeg')
    traverse(ffmpeg.parent, account, groups)
    listen = listener(args.listen)
    name = 'flussonix-' + args.instance + '.service'
    output = absolute(args.output)
    if output.is_relative_to(release / 'web'):
        raise ValueError('unit output cannot be inside the public web tree')
    if output.name != name:
        raise ValueError(f'output filename must be {name}')
    command = ' '.join('"' + str(value) + '"' for value in [release / 'bin/flussonix', '--listen', listen,
        '--config', state / 'config.json', '--media-dir', state / 'media', '--web-dir', release / 'web', '--ffmpeg', ffmpeg])
    unit = f'''# Generated by FlussoniX; instance={args.instance}
[Unit]
Description=FlussoniX preview ({args.instance})
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=30
StartLimitBurst=5

[Service]
Type=simple
User={account.pw_name}
Group={account.pw_gid}
WorkingDirectory={release}
EnvironmentFile={secret}
ExecStart={command}
Restart=on-failure
RestartSec=2
TimeoutStopSec=15
KillMode=control-group
UMask=0077
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=read-only
PrivateTmp=yes
ReadWritePaths={state}
LimitNOFILE=65536
SyslogIdentifier=flussonix-{args.instance}

[Install]
WantedBy=multi-user.target
'''
    create_file(output, unit.encode(), 0o644)
    print(f'Wrote {output}; review and install it explicitly with systemctl')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    credential = commands.add_parser('credentials', help='create a new protected credentials file')
    credential.add_argument('--output', required=True)
    credential.add_argument('--admin-user', default='admin')
    credential.add_argument('--from-env', action='store_true', help='read the three FLUSSONIX credential variables')
    service = commands.add_parser('render', help='verify paths and write a unit; never starts/enables services')
    for name in ['instance', 'release-dir', 'state-dir', 'credentials', 'user', 'listen', 'output']:
        service.add_argument('--' + name, required=True)
    service.add_argument('--ffmpeg', default='/usr/bin/ffmpeg')
    args = parser.parse_args()
    try:
        credentials(args) if args.command == 'credentials' else render(args)
    except (ValueError, OSError, KeyError) as error:
        parser.exit(1, f'Cannot prepare service: {error}\n')


if __name__ == '__main__':
    main()
