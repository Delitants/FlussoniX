#!/usr/bin/env python3
"""Keep local planning and reports out of source pushes and release uploads."""
import argparse,fnmatch,gzip,hashlib,io,json,pathlib,re,shutil,stat,subprocess,sys,tarfile,tempfile,zipfile
from contextlib import contextmanager
CONTROL='.github/publication/'
CONTROL_FILES={CONTROL+'guard.py',CONTROL+'install-hooks.py',CONTROL+'policy.json'}
PRIVATE_DIRS={'work','outputs','internal','reports','test-reports','evidence','acceptance','plans',
              'superpowers','.superpowers','.codex','.agents','.agent-artifacts','privatefixtures'}
PRIVATE_FILES={'release-info.json','release-evidence.json','host-matrix.json','development-evidence.json',
               'qualification-results.json','format-matrix.json','reference-actions.csv','reference-tools.csv',
               'results.json','test-results.xml','test-results.json','test-report.json','agents.md','claude.md','gemini.md'}
DOC_TYPES={'.md','.rst','.txt','.json','.csv','.pdf','.html','.xml'}
REPORT=re.compile(r'(?:^|[-_ .])(?:test[-_ ]?(?:reports?|results)|implementation[-_ ]?(?:plans?|notes)|planning|qualification|verification[-_ ]?report|development[-_ ]?(?:notes|evidence)|host[-_ ]?matrix)(?:[-_ .]|$)',re.I)
INTERNAL=re.compile(r'(?im)(?:/Users/[^\s/]+/|\b\d+\s+(?:(?:unit|integration|native)\s+)?tests?\s+(?:passed|failed|skipped)\b|^\s*(?:#+\s*)?(?:implementation plan|test results|test report|verification report|acceptance evidence|qualification results|post-signing checks|source commit|reviewed source|build log)\b)')
USER_DOCS={'readme.md','readme.txt','readme','install.md','install.txt','recovery.md','known-limits.md','changelog.md'}
PUBLIC_GUIDE_TYPES={'.md','.rst','.txt','.html'}
PACKAGE_DATA={'package.json','package-lock.json','composer.json','composer.lock','tsconfig.json','Cargo.lock'}
CODE_TYPES={'.go','.c','.h','.cc','.cpp','.cxx','.hpp','.swift','.py','.sh','.js','.ts','.php','.rs','.java','.m','.mm','.lua','.pl','.rb','.erl','.ex','.sql','.s','.asm'}
MAX_TEXT=1048576
MAX_ARCHIVE_MEMBERS=4096
MAX_ARCHIVE_UNCOMPRESSED=512*1024*1024
MAX_CHECKSUM=65536
MAX_DEBIAN=512*1024*1024
MAX_DEBIAN_FILES=16384
MAX_INSPECTION=16*1024*1024
CHECKSUM_LINE=re.compile(r'[0-9a-fA-F]{64}  (?:[A-Za-z0-9][A-Za-z0-9._+-]*\.tar\.gz(?:\.sha256)?|flussonix_[A-Za-z0-9.+:~+-]+_amd64\.deb(?:\.sha256|\.inspection\.json)?)')
GITHUB_ORIGIN=re.compile(
 r'(?:https://(?:[^@/?#\s]+@)?github\.com/|git@github\.com:|ssh://git@github\.com/)'
 r'([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?',re.I|re.ASCII)

def origin_matches_repository(origin,repository):
 match=GITHUB_ORIGIN.fullmatch(origin)
 return bool(match and match[1].casefold()==repository.casefold())

def private_error(name,policy=None):
 p=pathlib.PurePosixPath(name)
 if not name or not p.parts or p.is_absolute() or '..' in p.parts or '\\' in name:return 'unsafe path'
 parts=[x.removeprefix('._').casefold() for x in p.parts]
 if any(x in PRIVATE_DIRS and not (x=='internal' and (p.suffix.casefold() in CODE_TYPES or parts[-1]=='openapi.json')) for x in parts):return 'local-only directory'
 if parts[-1] in PRIVATE_FILES or p.suffix.casefold()=='.log':return 'local-only report/log'
 if p.suffix.casefold() in DOC_TYPES|{'.zip','.gz','.xz','.tar','.tgz'} and REPORT.search(parts[-1]):
  if not policy or name not in policy.get('public_docs',[]):return 'internal planning/report file'
 return None

def text_errors(name,text):
 return [name+': keep planning, test details and local paths out of public help/notes'] if INTERNAL.search(text) else []

def public_guide(name):
 return pathlib.PurePosixPath(name).suffix.casefold() in PUBLIC_GUIDE_TYPES

def artifact_path_value(step):
 lines=step.splitlines()
 for i,line in enumerate(lines):
  if not re.fullmatch(r'with:\s*(?:#.*)?',line.strip()):continue
  with_indent=len(line)-len(line.lstrip())
  for j in range(i+1,len(lines)):
   current=lines[j]
   if not current.strip() or current.lstrip().startswith('#'):continue
   indent=len(current)-len(current.lstrip())
   if indent<=with_indent:break
   match=re.match(r'\s+path:\s*(.*)$',current)
   if not match:continue
   value=match[1].strip()
   if not re.fullmatch(r'[|>](?:[1-9][+-]?|[+-][1-9]?)?(?:\s+#.*)?',value):return value
   path_indent=indent
   values=[]
   for following in lines[j+1:]:
    if following.strip() and len(following)-len(following.lstrip())<=path_indent:break
    if following.strip() and not following.lstrip().startswith('!'):values.append(following.strip())
   return '\n'.join(values)
 return None

def workflow_errors(name,text):
 errors=[]
 if re.search(r'\\(?:x[0-9A-Fa-f]{2}|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})',text):
  errors.append(name+': escaped YAML scalars could hide an upload action or path')
 for action in re.finditer(r'(?m)^\s*(?:-\s*)?uses\s*:\s*([^\n]*)',text):
  value=action[1].strip()
  if value.startswith('*') or '\\' in value or '${{' in value:
   errors.append(name+': workflow action must be a concrete literal')
 starts=list(re.finditer(r'(?m)^( +)-(?=\s|$)',text))
 recognized=[]
 for i,match in enumerate(starts):
  end=next((m.start() for m in starts[i+1:] if len(m[1])<=len(match[1])),len(text))
  step=text[match.start():end]
  if not re.search(r'(?m)^\s*(?:-\s*)?uses:\s*[^\n]*upload-artifact@',step):continue
  recognized.append((match.start(),end))
  visible=artifact_path_value(step)
  if visible is None:
   errors.append(name+': upload-artifact path must be explicit');continue
  if '\\' in visible or '${{' in visible or re.match(r'\s*[*&]',visible):
   errors.append(name+': upload-artifact path must be a concrete path');continue
  private_paths=any(re.search(r"(?:^|[/\s:'\"])"+re.escape(part)+r"""(?=/|\s|['",\]}]|$)""",visible,re.I) for part in PRIVATE_DIRS)
  if private_paths or re.search(r'(?i)(?:evidence|coverage[-_ ]?report|(?:qa|test)[-_ /](?:results?|reports?|logs?)|work/qa|\.log\b|junit[^\n]*\.xml)',visible):
   errors.append(name+': internal QA/test reports must stay local')
 for mention in re.finditer(r'upload-artifact@',text):
  if not any(start<=mention.start()<end for start,end in recognized):
   errors.append(name+': upload-artifact action must use an explicit step and path')
 return errors

def _yaml_code(line):
 out=[];quote=None;escaped=False
 for character in line:
  if quote=='"':
   out.append(character)
   if escaped:escaped=False
   elif character=='\\':escaped=True
   elif character=='"':quote=None
  elif quote=="'":
   out.append(character)
   if character=="'":quote=None
  elif character in "'\"":quote=character;out.append(character)
  elif character=='#':break
  else:out.append(character)
 return ''.join(out).rstrip()

def workflow_pin_errors(name,text):
 errors=[]
 for line in text.splitlines():
  code=_yaml_code(line)
  match=re.fullmatch(r'\s*(?:-\s*)?uses\s*:\s*(.*?)\s*',code)
  ambiguous=(re.match(r'\s*(?:-\s*)?["\']uses["\']\s*:',code)
             or re.search(r'[{,]\s*(?:uses|["\']uses["\'])\s*:',code)
             or re.match(r'\s*(?:-\s*)?\?(?:\s|$)',code)
             or re.match(r'\s*(?:-\s*)?\*[A-Za-z0-9_.-]+\s*:',code)
             or re.match(r'\s*(?:-\s*)?[!&][^\s]+\s+(?:uses|["\']uses["\'])\s*:',code))
  if not match:
   if ambiguous:errors.append(name+': workflow action must use a plain concrete uses key')
   continue
  value=match[1]
  if (len(value)>=2 and value[0]==value[-1] and value[0] in "'\""):
   value=value[1:-1]
  if (not value or value.startswith(('*','&','|','>')) or '\\' in value
      or '${{' in value or any(character.isspace() for character in value)):
   errors.append(name+': workflow action must be a concrete literal');continue
  if value.startswith('./'):
   continue
  if not re.fullmatch(
      r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*@[0-9A-Fa-f]{40}',
      value):
   errors.append(name+': external workflow action must use a full commit SHA')
 return errors

def git(repo,*args):return subprocess.check_output(['git','-C',str(repo),*args],stderr=subprocess.PIPE)

def exists_at(repo,revision,path):
 return subprocess.run(['git','-C',str(repo),'cat-file','-e',revision+':'+path],capture_output=True).returncode==0

def changed_errors(repo,policy,revision,index=False):
 base=policy['baseline']
 if index and subprocess.run(['git','-C',str(repo),'cat-file','-e',base+'^{commit}'],capture_output=True).returncode:
  base='HEAD' if subprocess.run(['git','-C',str(repo),'rev-parse','--verify','HEAD'],capture_output=True).returncode==0 else '4b825dc642cb6eb9a060e54bf8d69288fbee4904'
 command=('diff','--cached','--name-only','--diff-filter=ACMRT','-z',base) if index else ('diff-tree','--root','-m','--no-commit-id','--name-only','--diff-filter=ACMRT','-r','-z',revision)
 names=set(x.decode('utf-8') for x in git(repo,*command).split(b'\0') if x)
 errors=[]
 for name in sorted(names):
  if not index and not exists_at(repo,revision,name):continue
  error=private_error(name,policy)
  if error:errors.append(name+': '+error);continue
  p=pathlib.PurePosixPath(name)
  if p.suffix.casefold() in DOC_TYPES and name not in CONTROL_FILES and p.name not in PACKAGE_DATA:
   allowed=policy.get('public_data',[])+policy.get('public_docs',[])
   known=exists_at(repo,base,name)
   standard=bool(re.fullmatch(r'(?:README|CHANGELOG|LICENSE|COPYING|NOTICE)(?:\.[A-Za-z0-9_-]+)?',p.name,re.I))
   if not known and not standard and not any(fnmatch.fnmatchcase(name,x) for x in allowed):
    errors.append(name+': new document/data path needs public-policy approval');continue
  if name.startswith('.github/workflows/') and p.suffix.casefold() in {'.yml','.yaml'}:
   errors+=workflow_errors(name,git(repo,'show',(':' if index else revision+':')+name).decode('utf-8'))
  historical_exception=not index and name in policy.get('historical_public_guide_exceptions',{}).get(revision,[])
  scan_public_guide=public_guide(name) and not historical_exception
  if index and scan_public_guide and exists_at(repo,'HEAD',name):
   scan_public_guide=git(repo,'show',':'+name)!=git(repo,'show','HEAD:'+name)
  if (p.name.casefold() in USER_DOCS and p.suffix.casefold()!='.json') or scan_public_guide:
   value=git(repo,'show',(':' if index else revision+':')+name)
   if len(value)>MAX_TEXT:errors.append(name+': help file exceeds size limit')
   else:errors+=text_errors(name,value.decode('utf-8'))
 return errors

def final_workflow_pin_errors(repo,revision=None,index=False):
 if index:
  names=git(repo,'ls-files','--cached','-z','--','.github/workflows')
 else:
  names=git(repo,'ls-tree','-r','--name-only','-z',revision,'--','.github/workflows')
 errors=[]
 for raw in names.split(b'\0'):
  if not raw:continue
  name=raw.decode('utf-8')
  if pathlib.PurePosixPath(name).suffix.casefold() not in {'.yml','.yaml'}:continue
  value=git(repo,'show',(':' if index else revision+':')+name).decode('utf-8')
  errors+=workflow_pin_errors(name,value)
 return errors

def final_public_guide_errors(repo,policy,revision):
 errors=[]
 for name in sorted(set(policy.get('public_docs',[]))):
  path=pathlib.PurePosixPath(name)
  if path.suffix.casefold() not in PUBLIC_GUIDE_TYPES or not exists_at(repo,revision,name):continue
  value=git(repo,'show',revision+':'+name)
  if len(value)>MAX_TEXT:errors.append(name+': help file exceeds size limit')
  else:errors+=text_errors(name,value.decode('utf-8'))
 return errors

def index_errors(repo,policy):
 return changed_errors(repo,policy,None,index=True)+final_workflow_pin_errors(repo,index=True)

def history_errors(repo,policy,revision):
 try:
  base=policy['baseline']
  if subprocess.run(['git','-C',str(repo),'merge-base','--is-ancestor',base,revision],capture_output=True).returncode:
   return ['Update/rebase from the protected public baseline before publishing.']
  errors=[]
  for commit in git(repo,'rev-list',revision,'^'+base).decode().splitlines():
   errors += [commit[:12]+' '+e for e in changed_errors(repo,policy,commit)]
  errors += final_public_guide_errors(repo,policy,revision)
  errors += final_workflow_pin_errors(repo,revision)
  return errors
 except (OSError,ValueError,UnicodeError,subprocess.CalledProcessError) as e:return ['Cannot verify source: '+str(e)]

def archive_errors(path):
 errors=[]
 def entry(name,size,reader):
  error=private_error(name+'__directory__.c' if name.endswith('/') else name)
  if error:errors.append(name+': '+error)
  elif reader and (pathlib.PurePosixPath(name).name.casefold() in USER_DOCS or pathlib.PurePosixPath(name).suffix.casefold() in PUBLIC_GUIDE_TYPES):
   if size>MAX_TEXT:errors.append(name+': help exceeds size limit')
   else:errors.extend(text_errors(name,reader().decode('utf-8')))
 try:
  expanded=0
  with gzip.open(path,'rb') as source:
   while data:=source.read(65536):
    expanded+=len(data)
    if expanded>MAX_ARCHIVE_UNCOMPRESSED:
     errors.append(str(path)+': archive exceeds uncompressed size limit');return errors
  with tarfile.open(path,'r:gz') as tar:
   for index,item in enumerate(tar):
    if index>=MAX_ARCHIVE_MEMBERS:
     errors.append(str(path)+': archive has too many members');break
    if item.issym() or item.islnk():
     error=private_error(item.linkname)
     if error:errors.append(item.name+': unsafe or private archive link')
    entry(item.name+'/' if item.isdir() else item.name,item.size,(lambda i=item:tar.extractfile(i).read(MAX_TEXT+1)) if item.isfile() else None)
   tar_end=tar.offset
  with gzip.open(path,'rb') as source:
   source.seek(tar_end)
   while data:=source.read(65536):
    if any(data):
     errors.append(str(path)+': archive has nonzero data after tar end marker');break
 except (OSError,ValueError,UnicodeError,RuntimeError,EOFError,tarfile.TarError) as e:errors.append(str(path)+': '+str(e))
 return errors

def checksum_errors(path):
 try:
  if path.stat().st_size>MAX_CHECKSUM:return [str(path)+': checksum file exceeds size limit']
  with path.open('rb') as source:data=source.read(MAX_CHECKSUM+1)
  if len(data)>MAX_CHECKSUM:return [str(path)+': checksum file exceeds size limit']
  text=data.decode('ascii')
  lines=text.removesuffix('\n').split('\n')
  if not text or not all(CHECKSUM_LINE.fullmatch(line) for line in lines):
   return [str(path)+': invalid checksum lines']
  return []
 except (OSError,UnicodeError) as error:return [str(path)+': '+str(error)]

def bounded_regular(path,limit):
 if path.is_symlink() or not path.is_file() or path.stat().st_size>limit:
  raise ValueError(str(path)+': missing, linked or oversized file')
 with path.open('rb') as source:
  value=source.read(limit+1)
 if len(value)>limit:raise ValueError(str(path)+': file exceeds size limit')
 return value

def release_archive_identity(path):
 manifests=[]
 try:
  with tarfile.open(path,'r:gz') as bundle:
   for member in bundle:
    parts=pathlib.PurePosixPath(member.name).parts
    if len(parts)==2 and parts[1]=='manifest.json' and member.isfile():
     source=bundle.extractfile(member)
     if source is None or member.size>MAX_INSPECTION:raise ValueError('release manifest is invalid')
     manifests.append(json.loads(source.read(MAX_INSPECTION+1)))
 except (OSError,UnicodeError,json.JSONDecodeError,tarfile.TarError) as error:
  raise ValueError('release manifest cannot be read') from error
 if len(manifests)!=1:raise ValueError('release archive must contain one top-level manifest')
 manifest=manifests[0]
 if (not isinstance(manifest,dict) or manifest.get('format')!=1
     or manifest.get('name')!='FlussoniX'
     or not re.fullmatch(r'[0-9][0-9A-Za-z.+~-]*',str(manifest.get('version','')))
     or not re.fullmatch(r'[0-9a-f]{40}',str(manifest.get('source_revision','')))
     or manifest.get('architecture')!='x86_64'
     or not isinstance(manifest.get('files'),dict)):
  raise ValueError('release manifest identity is invalid')
 return manifest

def debian_ar_members(path):
 data=bounded_regular(path,MAX_DEBIAN)
 if not data.startswith(b'!<arch>\n'):raise ValueError('Debian package lacks ar magic')
 position=8;members={}
 while position<len(data):
  if len(data)-position<60:raise ValueError('Debian package has a truncated ar header')
  header=data[position:position+60];position+=60
  if header[58:60]!=b'`\n':raise ValueError('Debian package has an invalid ar header')
  try:
   name=header[:16].decode('ascii').strip().removesuffix('/')
   size=int(header[48:58].decode('ascii').strip())
  except (UnicodeDecodeError,ValueError) as error:
   raise ValueError('Debian package has invalid ar metadata') from error
  if (not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.+-]*',name) or name in members
      or size<0 or size>MAX_DEBIAN or position+size>len(data)):
   raise ValueError('Debian package has unsafe ar members')
  members[name]=data[position:position+size];position+=size
  if size%2:
   if position>=len(data) or data[position:position+1]!=b'\n':
    raise ValueError('Debian package has invalid ar padding')
   position+=1
 expected={'debian-binary'}
 controls=[name for name in members if name.startswith('control.tar.')]
 payloads=[name for name in members if name.startswith('data.tar.')]
 if (members.get('debian-binary')!=b'2.0\n' or len(controls)!=1 or len(payloads)!=1
     or set(members)!=(expected|set(controls)|set(payloads))
     or pathlib.PurePosixPath(controls[0]).suffix not in {'.gz','.xz'}
     or pathlib.PurePosixPath(payloads[0]).suffix not in {'.gz','.xz'}):
  raise ValueError('Debian package ar members are not the supported format')
 return members[controls[0]],members[payloads[0]]

def safe_debian_path(name):
 path=pathlib.PurePosixPath(name.removeprefix('./'))
 if path.is_absolute() or not path.parts or any(part in ('','.','..') for part in path.parts):
  raise ValueError('Debian package contains an unsafe path')
 return '/'+path.as_posix()

def tar_regular_files(data,control=False):
 files={};control_bytes=None
 try:
  with tarfile.open(fileobj=io.BytesIO(data),mode='r:*') as bundle:
   for index,member in enumerate(bundle):
    if index>=MAX_DEBIAN_FILES:raise ValueError('Debian package contains too many files')
    if member.isdir():continue
    if not member.isfile() or member.issym() or member.islnk() or member.size>MAX_DEBIAN:
     raise ValueError('Debian package contains linked, special or oversized content')
    path=safe_debian_path(member.name)
    if path in files:raise ValueError('Debian package contains duplicate files')
    source=bundle.extractfile(member)
    if source is None:raise ValueError('Debian package member cannot be read')
    payload=source.read(MAX_DEBIAN+1)
    if len(payload)!=member.size:raise ValueError('Debian package member size changed')
    files[path]={'sha256':hashlib.sha256(payload).hexdigest(),'bytes':len(payload),
                 'mode':oct(member.mode&0o777)}
    if control and path=='/control':control_bytes=payload
 except (OSError,EOFError,tarfile.TarError) as error:
  raise ValueError('Debian package contains an invalid tar member') from error
 if control and control_bytes is None:raise ValueError('Debian package control file is missing')
 return control_bytes if control else files

def debian_control(data):
 try:text=data.decode('utf-8')
 except UnicodeDecodeError as error:raise ValueError('Debian control is not UTF-8') from error
 fields={};current=None
 for line in text.splitlines():
  if line.startswith((' ','\t')) and current:
   fields[current]+=' '+line.strip();continue
  if ': ' not in line:raise ValueError('Debian control has invalid fields')
  current,value=line.split(': ',1)
  if current in fields:raise ValueError('Debian control has duplicate fields')
  fields[current]=value
 return fields

def package_release_errors(package,checksum,inspection,archive,manifest,version):
 errors=[]
 try:
  expected_name=f'flussonix_{version}-1_amd64.deb'
  if package.name!=expected_name or checksum.name!=expected_name+'.sha256' or inspection.name!=expected_name+'.inspection.json':
   raise ValueError('Debian release filenames do not match the release version')
  checksum_problems=checksum_errors(checksum)
  if checksum_problems:raise ValueError('; '.join(checksum_problems))
  digest=hashlib.sha256(bounded_regular(package,MAX_DEBIAN)).hexdigest()
  line=bounded_regular(checksum,MAX_CHECKSUM).decode('ascii').strip().split('  ',1)
  if line!=[digest,package.name]:raise ValueError('Debian package checksum mismatch')
  report=json.loads(bounded_regular(inspection,MAX_INSPECTION))
  metadata=report.get('package') if isinstance(report,dict) else None
  archive_row=report.get('archive') if isinstance(report,dict) else None
  expected_files=report.get('files') if isinstance(report,dict) else None
  archive_digest=hashlib.sha256(bounded_regular(archive,MAX_ARCHIVE_UNCOMPRESSED)).hexdigest()
  if (report.get('format')!=1 or not isinstance(metadata,dict)
      or not isinstance(archive_row,dict) or not isinstance(expected_files,dict)
      or metadata.get('name')!='flussonix' or metadata.get('version')!=version+'-1'
      or metadata.get('architecture')!='amd64'
      or archive_row.get('sha256')!=archive_digest
      or archive_row.get('source_revision')!=manifest['source_revision']
      or archive_row.get('version')!=version or archive_row.get('architecture')!='x86_64'):
   raise ValueError('Debian inspection identity differs from the accepted release archive')
  control_tar,data_tar=debian_ar_members(package)
  control=debian_control(tar_regular_files(control_tar,control=True))
  if (control.get('Package')!='flussonix' or control.get('Version')!=version+'-1'
      or control.get('Architecture')!='amd64'):
   raise ValueError('Debian control identity is invalid')
  dependencies=[value.strip() for value in control.get('Depends','').split(',') if value.strip()]
  recommends=[value.strip() for value in control.get('Recommends','').split(',') if value.strip()]
  if metadata.get('depends')!=dependencies or metadata.get('recommends')!=recommends:
   raise ValueError('Debian control dependencies differ from inspection')
  if tar_regular_files(data_tar)!=expected_files:
   raise ValueError('Debian payload differs from inspection')
 except (OSError,UnicodeError,json.JSONDecodeError,ValueError) as error:
  errors.append(package.name+': '+str(error))
 return errors

def release_errors(policy,tag,assets,notes=None):
 if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._+-]*',tag):return ['Unsafe release tag']
 version=tag[1:] if tag.startswith('v') else tag
 patterns=[p.replace('{version}',version) for p in policy.get('release_assets',[])]
 errors=[];seen=set();archives={};checksums={};packages={};inspections={}
 for value in assets:
  p=pathlib.Path(value);name=p.name
  if name in seen:errors.append('Duplicated release asset: '+name)
  seen.add(name)
  error=private_error(name)
  if error:errors.append(name+': '+error);continue
  if not any(fnmatch.fnmatchcase(name,pattern) for pattern in patterns):errors.append(name+': not an approved release asset');continue
  if p.is_symlink() or not p.is_file():errors.append(name+': missing/unsafe asset');continue
  if name.endswith('.tar.gz'):
   archive_problems=archive_errors(p)
   errors+=archive_problems
   if not archive_problems:archives[name]=p
  elif name.endswith('.tar.gz.sha256') or name.endswith('.deb.sha256') or name=='SHA256SUMS':
   checksum_problems=checksum_errors(p)
   errors+=checksum_problems
   if not checksum_problems:checksums[name]=p
  elif name.endswith('.deb'):
   packages[name]=p
  elif name.endswith('.deb.inspection.json'):
   try:bounded_regular(p,MAX_INSPECTION);inspections[name]=p
   except ValueError as problem:errors.append(str(problem))
  else:errors.append(name+': release asset type has no validator')
  if name.casefold() in USER_DOCS:
   if p.stat().st_size>MAX_TEXT:errors.append(name+': help exceeds size limit')
   else:errors+=text_errors(name,p.read_text(encoding='utf-8'))
 for name,archive in archives.items():
  sidecar=checksums.get(name+'.sha256')
  manifest=checksums.get('SHA256SUMS')
  if not sidecar and not manifest:
   errors.append(name+': missing checksum sidecar or SHA256SUMS');continue
  with archive.open('rb') as source:digest=hashlib.file_digest(source,'sha256').hexdigest()
  for checksum_file in (sidecar,manifest):
   if checksum_file is None:continue
   lines=[line.split('  ',1) for line in checksum_file.read_text(encoding='ascii').splitlines()]
   matching=[value for value,filename in lines if filename==name]
   if len(matching)!=1:
    errors.append(checksum_file.name+': checksum must name '+name+' exactly once')
   elif matching[0].lower()!=digest:
    errors.append(checksum_file.name+': checksum mismatch for '+name)
 if policy.get('require_debian_package'):
  expected_package=f'flussonix_{version}-1_amd64.deb'
  if len(archives)!=1:
   errors.append('Release must contain exactly one accepted x86_64 archive')
  if set(packages)!={expected_package}:
   errors.append('Release must contain exactly one version-matched amd64 Debian package')
  package_checksum=checksums.get(expected_package+'.sha256')
  inspection=inspections.get(expected_package+'.inspection.json')
  if package_checksum is None:errors.append(expected_package+': missing checksum sidecar')
  if inspection is None:errors.append(expected_package+': missing inspection record')
  if len(archives)==1 and expected_package in packages and package_checksum and inspection:
   archive=next(iter(archives.values()))
   try:
    manifest=release_archive_identity(archive)
    if manifest['version']!=version:raise ValueError('release archive version differs from tag')
    expected_archive=f'flussonix-{version}-linux-x86_64-{manifest["source_revision"][:12]}.tar.gz'
    if archive.name!=expected_archive:raise ValueError('release archive filename differs from manifest')
    errors+=package_release_errors(packages[expected_package],package_checksum,
                                   inspection,archive,manifest,version)
   except ValueError as problem:errors.append(archive.name+': '+str(problem))
 if notes:
  p=pathlib.Path(notes)
  if p.is_symlink() or not p.is_file() or p.stat().st_size>MAX_TEXT:errors.append('Missing/unsafe release notes')
  else:errors+=text_errors('Release notes',p.read_text(encoding='utf-8'))
 return errors

def verify_ci_metadata(run,jobs,artifacts,run_id,tag,revision,repository):
 if not all(isinstance(value,dict) for value in (run,jobs,artifacts)):
  raise ValueError('Qualified CI metadata is invalid')
 version=tag[1:] if tag.startswith('v') else tag
 archive=f'flussonix-{version}-linux-x86_64-{revision[:12]}.tar.gz'
 package=f'flussonix_{version}-1_amd64.deb'
 artifact_name=f'flussonix-candidate-{revision}'
 if (run.get('status')!='completed' or run.get('conclusion')!='success'
     or run.get('event')!='push' or run.get('head_branch')!='main'
     or run.get('head_sha')!=revision or run.get('name')!='check'
     or run.get('path') not in ('.github/workflows/ci.yml',
                                '.github/workflows/ci.yml@main')
     or not isinstance(run.get('repository'),dict)
     or run['repository'].get('full_name')!=repository
     or type(run['repository'].get('id')) is not int):
  raise ValueError('Qualified CI run does not match the release source')
 rows=jobs.get('jobs')
 if (not isinstance(rows,list) or jobs.get('total_count')!=len(rows)
     or len(rows)!=2 or {row.get('name') for row in rows if isinstance(row,dict)}
     != {'verify','release_package_smoke'}
     or any(not isinstance(row,dict) or row.get('status')!='completed'
            or row.get('conclusion')!='success' for row in rows)):
  raise ValueError('Qualified CI jobs did not both pass')
 rows=artifacts.get('artifacts')
 if not isinstance(rows,list) or artifacts.get('total_count')!=1 or len(rows)!=1:
  raise ValueError('Qualified CI artifact is missing or ambiguous')
 row=rows[0]
 if (not isinstance(row,dict) or type(row.get('id')) is not int or row['id']<=0
     or not isinstance(row.get('digest'),str)
     or not re.fullmatch(r'sha256:[0-9a-f]{64}',row['digest'])
     or row.get('name')!=artifact_name
     or row.get('expired') is not False
     or type(row.get('size_in_bytes')) is not int
     or not 0<row['size_in_bytes']<=MAX_ARCHIVE_UNCOMPRESSED
     or not isinstance(row.get('workflow_run'),dict)
     or row['workflow_run'].get('id')!=run_id
     or row['workflow_run'].get('head_sha')!=revision
     or row['workflow_run'].get('repository_id')!=run['repository']['id']):
  raise ValueError('Qualified CI artifact does not match the release run')
 return (artifact_name,archive,archive+'.sha256',package,package+'.sha256',
         package+'.inspection.json')

def verify_ci_zip(downloaded,assets,archive,digest,size):
 match=re.fullmatch(r'flussonix-(.+)-linux-x86_64-[0-9a-f]{12}\.tar\.gz',archive)
 if not match:raise ValueError('Qualified CI archive name is invalid')
 package=f'flussonix_{match[1]}-1_amd64.deb'
 names={archive,archive+'.sha256',package,package+'.sha256',
        package+'.inspection.json'}
 if len(assets)!=5 or {path.name for path in assets}!=names:
  raise ValueError('Upload must contain the exact CI archive and Debian package sidecars')
 if (downloaded.is_symlink() or not downloaded.is_file()
     or downloaded.stat().st_size!=size or size>MAX_ARCHIVE_UNCOMPRESSED):
  raise ValueError('Qualified CI ZIP size differs from GitHub metadata')
 with downloaded.open('rb') as source:
  if 'sha256:'+hashlib.file_digest(source,'sha256').hexdigest()!=digest:
   raise ValueError('Qualified CI ZIP digest differs from GitHub metadata')
 try:
  with zipfile.ZipFile(downloaded) as bundle:
   entries=bundle.infolist()
   if len(entries)!=5 or {item.filename for item in entries}!=names:
    raise ValueError('Downloaded CI artifact has unexpected files')
   for item in entries:
    if item.filename.endswith('.sha256'):limit=MAX_CHECKSUM
    elif item.filename.endswith('.inspection.json'):limit=MAX_INSPECTION
    else:limit=MAX_ARCHIVE_UNCOMPRESSED
    mode=stat.S_IFMT(item.external_attr>>16)
    if (item.is_dir() or item.flag_bits&1 or mode not in (0,stat.S_IFREG)
        or item.file_size>limit):
     raise ValueError('Qualified CI artifact contains unsafe files')
    local=next(path for path in assets if path.name==item.filename)
    if (local.is_symlink() or not local.is_file() or local.stat().st_size!=item.file_size):
     raise ValueError('Upload differs from qualified CI artifact')
    with bundle.open(item) as remote,local.open('rb') as source:
     remote_hash=hashlib.sha256();total=0
     while block:=remote.read(65536):
      total+=len(block)
      if total>limit:raise ValueError('Qualified CI entry exceeds size limit')
      remote_hash.update(block)
     if total!=item.file_size or remote_hash.digest()!=hashlib.file_digest(source,'sha256').digest():
      raise ValueError('Upload differs from qualified CI artifact')
 except (zipfile.BadZipFile,RuntimeError,EOFError) as error:
  raise ValueError('Qualified CI ZIP is invalid') from error

def github_json(repository,path):
 return json.loads(subprocess.check_output(['gh','api',path]))

def remote_tag_revision(repository,tag):
 ref=github_json(repository,f'repos/{repository}/git/ref/tags/{tag}')
 value=ref.get('object') if isinstance(ref,dict) else None
 for _ in range(4):
  if not isinstance(value,dict) or not re.fullmatch(r'[0-9a-f]{40}',str(value.get('sha',''))):
   break
  if value.get('type')=='commit':return value['sha']
  if value.get('type')!='tag':break
  tagged=github_json(repository,f'repos/{repository}/git/tags/{value["sha"]}')
  value=tagged.get('object') if isinstance(tagged,dict) else None
 raise ValueError('Remote release tag does not resolve to a commit')

def download_ci_zip(repository,artifact_id,destination):
 with destination.open('xb') as output:
  subprocess.run(['gh','api',f'repos/{repository}/actions/artifacts/{artifact_id}/zip'],
                 stdout=output,check=True)

def verify_ci_provenance(repo,policy,tag,assets,run_id):
 if (type(run_id) is not int or run_id<=0
     or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._+-]*',tag)):
  raise ValueError('Qualified CI run ID or release tag is invalid')
 repository=policy['repository']
 revision=git(repo,'rev-parse','HEAD').decode().strip()
 if not re.fullmatch(r'[0-9a-f]{40}',revision):
  raise ValueError('Release checkout revision is invalid')
 local_tag=git(repo,'rev-parse','--verify',f'refs/tags/{tag}^{{commit}}').decode().strip()
 if local_tag!=revision or remote_tag_revision(repository,tag)!=revision:
  raise ValueError('Release tag differs from qualified source revision')
 root=f'repos/{repository}/actions/runs/{run_id}'
 run=github_json(repository,root)
 jobs=github_json(repository,root+'/jobs?per_page=100')
 artifacts=github_json(repository,root+'/artifacts?per_page=100')
 metadata=verify_ci_metadata(run,jobs,artifacts,run_id,tag,revision,repository)
 archive=metadata[1]
 artifact=artifacts['artifacts'][0]
 common=(pathlib.Path(repo)/git(repo,'rev-parse','--git-common-dir').decode().strip()).resolve()
 with tempfile.TemporaryDirectory(prefix='publication-qualified-',dir=common) as temporary:
  downloaded=pathlib.Path(temporary)/'artifact.zip'
  download_ci_zip(repository,artifact['id'],downloaded)
  verify_ci_zip(downloaded,assets,archive,artifact['digest'],artifact['size_in_bytes'])

@contextmanager
def upload_snapshot(repo,policy,tag,assets,notes):
 common=(pathlib.Path(repo)/git(repo,'rev-parse','--git-common-dir').decode().strip()).resolve()
 stage=pathlib.Path(tempfile.mkdtemp(prefix='publication-upload-',dir=common));paths=[]
 try:
  for asset in assets:
   destination=stage/pathlib.Path(asset).name
   if destination.exists():raise ValueError('Duplicate upload filename')
   shutil.copyfile(asset,destination,follow_symlinks=False);paths.append(destination)
  note=None
  if notes:
   note=stage/'upload-notes.txt';shutil.copyfile(notes,note,follow_symlinks=False)
  errors=release_errors(policy,tag,paths,note)
  if errors:raise ValueError('; '.join(errors))
  yield paths,note
 finally:
  shutil.rmtree(stage)

def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--repository',type=pathlib.Path,default=pathlib.Path.cwd());p.add_argument('--policy',type=pathlib.Path)
 sub=p.add_subparsers(dest='command',required=True)
 sub.add_parser('index');h=sub.add_parser('history');h.add_argument('revision',nargs='?',default='HEAD')
 sub.add_parser('pre-push')
 for mode in ['release-files','upload']:
  r=sub.add_parser(mode);r.add_argument('--tag',required=True);r.add_argument('--asset',action='append',type=pathlib.Path,required=True);r.add_argument('--notes-file',type=pathlib.Path)
  if mode=='upload':r.add_argument('--qualified-run',type=int)
 args=p.parse_args();repo=args.repository.resolve()
 try:
  policy=json.loads((args.policy or repo/(CONTROL+'policy.json')).read_text())
  if args.command=='index':errors=index_errors(repo,policy)
  elif args.command=='history':errors=history_errors(repo,policy,args.revision)
  elif args.command=='pre-push':
   errors=[]
   for line in sys.stdin:
    fields=line.split()
    if len(fields)!=4:errors.append('Invalid pre-push input');continue
    if fields[1]=='0'*40:continue
    if not re.fullmatch('[0-9a-f]{40}',fields[1]):errors.append('Invalid push revision');continue
    errors+=history_errors(repo,policy,fields[1])
  else:
   errors=release_errors(policy,args.tag,args.asset,args.notes_file)
   if args.command=='upload':
    errors+=history_errors(repo,policy,'HEAD')
    if git(repo,'status','--porcelain').strip():errors.append('Upload requires a clean source checkout')
    if args.qualified_run is None:errors.append('Qualified CI run ID required for upload')
  if errors:
   for error in errors:print('Publication blocked: '+error,file=sys.stderr)
   return 1
  if args.command=='upload':
   # Preserve existing project signing/qualification gates; this checks the upload only.
   origin=git(repo,'remote','get-url','origin').decode().removesuffix('\n')
   if not origin_matches_repository(origin,policy['repository']):raise ValueError('Upload repository identity differs')
   with upload_snapshot(repo,policy,args.tag,args.asset,args.notes_file) as (assets,notes):
    verify_ci_provenance(repo,policy,args.tag,assets,args.qualified_run)
    release=json.loads(subprocess.check_output([
     'gh','release','view',args.tag,'--repo',policy['repository'],
     '--json','body,assets,isDraft,tagName']))
    if text_errors('Release notes',release.get('body') or ''):raise ValueError('Existing release notes contain internal details')
    version=args.tag[1:] if args.tag.startswith('v') else args.tag
    patterns=[p.replace('{version}',version) for p in policy.get('release_assets',[])]
    for asset in release['assets']:
     if private_error(asset['name']) or not any(fnmatch.fnmatchcase(asset['name'],p) for p in patterns):raise ValueError('Existing release has an unapproved attachment; review it before upload')
    subprocess.run(['gh','release','upload',args.tag,*[str(x) for x in assets],'--repo',policy['repository']],check=True)
  return 0
 except (OSError,ValueError,UnicodeError,KeyError,TypeError,subprocess.CalledProcessError) as error:
  print('Publication blocked: '+str(error),file=sys.stderr);return 1

if __name__=='__main__':sys.exit(main())
