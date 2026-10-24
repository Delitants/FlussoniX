"""Create a private traced source copy. Never patches the checkout or registry."""
from pathlib import Path
import argparse,hashlib,json,shutil
from checkpoint_hooks import install_checkpoint_hooks

def edit(path,old,new):
    s=path.read_text();assert s.count(old)==1,(str(path),old[:100],s.count(old));path.write_text(s.replace(old,new))
def validate_output(repo,out):
    assert not out.exists() and not repo.is_relative_to(out) and not out.is_relative_to(repo)

def main():
    p=argparse.ArgumentParser();p.add_argument('output',type=Path);p.add_argument('--quinn-source',type=Path,required=True);p.add_argument('--shaper-source',type=Path,required=True);a=p.parse_args()
    here=Path(__file__).resolve().parent;repo=here.parents[1];out=a.output.resolve()
    validate_output(repo,out)
    out.mkdir(parents=True)
    for name in ['Cargo.toml','Cargo.lock']:shutil.copy2(repo/name,out/name)
    for name in ['src','vendor','tests']:shutil.copytree(repo/name,out/name)
    shutil.copyfile(here/'transport_test.rs',out/'tests/packet_diagnostics.rs')
    q=out/'vendor/quinn';shutil.copytree(a.quinn_source,q)
    core=out/'vendor/fx-packet-trace';shutil.copytree(here/'trace-core',core,ignore=shutil.ignore_patterns('target','Cargo.lock'))
    edit(core/'Cargo.toml','[workspace]\n','')
    edit(out/'Cargo.toml','members = [".", "vendor/quinn-proto"]','members = [".", "vendor/quinn-proto", "vendor/fx-packet-trace"]\nexclude = ["vendor/quinn"]')
    edit(out/'Cargo.toml','[patch.crates-io]','[patch.crates-io]\nquinn = { path = "vendor/quinn" }')
    edit(out/'Cargo.toml','[dependencies]\n','[dependencies]\nfx-packet-trace = { path = "vendor/fx-packet-trace" }\n')
    edit(out/'Cargo.toml','[dev-dependencies]\n','[dev-dependencies]\nrcgen = "0.14.10"\n')
    for crate in ['quinn','quinn-proto']:
        path=out/'vendor'/crate/'Cargo.toml'
        with path.open('a') as f:f.write('\n[dependencies.fx-packet-trace]\npath = "../fx-packet-trace"\n')
    c=out/'vendor/quinn-proto/src/connection/mod.rs'
    edit(c,'    pub fn stats(&self) -> ConnectionStats {','    /// Private diagnostic identity; never serialized on the wire.\n    pub fn diagnostic_tag(&self) -> [u8;16] { if fx_packet_trace::enabled() {fx_packet_trace::tag(self.orig_rem_cid.as_ref())} else {[0;16]} }\n\n    /// Connection statistics.\n    pub fn stats(&self) -> ConnectionStats {')
    s=c.read_text();start=s.index('    pub fn poll_transmit(');body=s.index(') -> Option<Transmit> {',start)+len(') -> Option<Transmit> {');s=s[:body]+'\n        let _diagnostic_batch=fx_packet_trace::batch(self.diagnostic_tag());'+s[body:];c.write_text(s)
    edit(c,'        self.stats.udp_tx.on_sent(num_datagrams as u64, buf.len());','        fx_packet_trace::udp(fx_packet_trace::stage::SEGMENT,self.diagnostic_tag(),buf,if num_datagrams>1 {Some(segment_size)} else {None});\n        self.stats.udp_tx.on_sent(num_datagrams as u64, buf.len());')
    edit(c,'        let mut sent = SentFrames::default();\n        let space =','        let diagnostic_tag=self.diagnostic_tag();\n        let mut sent = SentFrames::default();\n        let space =')
    edit(c,'            match self.datagrams.write(buf, max_size) {','            let diagnostic_data=if fx_packet_trace::enabled() {self.datagrams.front_data().cloned()} else {None};\n            match self.datagrams.write(buf, max_size) {')
    edit(c,'                    sent_datagrams = true;','                    if let Some(data)=diagnostic_data {fx_packet_trace::emit(fx_packet_trace::stage::FRAME,diagnostic_tag,[pn,space_id as u64,0],&data);}\n                    sent_datagrams = true;')
    edit(c,'                Frame::Datagram(datagram) => {\n                    if self','                Frame::Datagram(datagram) => {\n                    let tag=self.diagnostic_tag();\n                    fx_packet_trace::emit(fx_packet_trace::stage::RX_FRAME,tag,[number,2,0],&datagram.data);\n                    if self')
    edit(c,'.received(datagram, &self.config.datagram_receive_buffer_size)?','.received_traced(datagram, &self.config.datagram_receive_buffer_size, tag)?')
    b=out/'vendor/quinn-proto/src/connection/packet_builder.rs'
    edit(b,'        let len = buffer.len() - encode_start;','        let len = buffer.len() - encode_start;\n        fx_packet_trace::emit(fx_packet_trace::stage::PACKET,conn.diagnostic_tag(),[self.exact_number,self.space as u64,encode_start as u64],&buffer[encode_start..]);')
    d=out/'vendor/quinn-proto/src/connection/datagrams.rs'
    edit(d,'        self.conn.datagrams.outgoing.push_back(Datagram { data });','        fx_packet_trace::emit(fx_packet_trace::stage::QUEUE,self.conn.diagnostic_tag(),[u64::from(drop),0,0],&data);\n        self.conn.datagrams.outgoing.push_back(Datagram { data });')
    edit(d,'        self.conn.datagrams.recv()','        let data=self.conn.datagrams.recv()?;\n        fx_packet_trace::emit(fx_packet_trace::stage::DEQUEUE,self.conn.diagnostic_tag(),[0;3],&data);\n        Some(data)')
    marker='    pub(super) fn received(\n'
    edit(d,marker,'    pub(super) fn front_data(&self)->Option<&Bytes> {self.outgoing.queue.front().map(|d|&d.data)}\n\n'+'    #[cfg(test)]\n'+marker)
    s=d.read_text();needle='        let window = match window {';ix=s.index(needle,s.index(marker));s=s[:ix]+'        self.received_traced(datagram,window,[0;16])\n    }\n    pub(super) fn received_traced(&mut self, datagram:Datagram, window:&Option<usize>, tag:[u8;16])->Result<bool,TransportError> {\n'+s[ix:];d.write_text(s)
    edit(d,'            self.recv();','            if let Some(data)=self.recv() {fx_packet_trace::emit(fx_packet_trace::stage::RX_DROP,tag,[0;3],&data);}')
    runtime=q/'src/connection.rs'
    edit(runtime,'                self.buffered_transmit = Some(t);\n                return Ok(false);','                fx_packet_trace::udp(fx_packet_trace::stage::PENDING,self.inner.diagnostic_tag(),&self.send_buffer[..t.size],t.segment_size);\n                self.buffered_transmit = Some(t);\n                return Ok(false);')
    edit(runtime,'                Ok(()) => false,\n                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => true,\n                Err(e) => return Err(e),','                Ok(()) => {fx_packet_trace::udp(fx_packet_trace::stage::ACCEPTED,self.inner.diagnostic_tag(),&self.send_buffer[..len],t.segment_size);false},\n                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {fx_packet_trace::udp(fx_packet_trace::stage::WOULD_BLOCK,self.inner.diagnostic_tag(),&self.send_buffer[..len],t.segment_size);true},\n                Err(e) => {fx_packet_trace::udp(fx_packet_trace::stage::SOCKET_ERROR,self.inner.diagnostic_tag(),&self.send_buffer[..len],t.segment_size);return Err(e)},')
    endpoint=q/'src/endpoint.rs'
    edit(endpoint,'                            let buf = data.split_to(meta.stride.min(data.len()));','                            let buf = data.split_to(meta.stride.min(data.len()));\n                            fx_packet_trace::emit(fx_packet_trace::stage::INGRESS,[0;16],[0;3],&buf);')
    edit(out/'src/main.rs','async fn main() -> Result<(), Box<dyn std::error::Error>> {','async fn main() -> Result<(), Box<dyn std::error::Error>> {\n    let _packet_trace=fx_packet_trace::ExportGuard::from_env();')
    edit(out/'src/fxlink/listener.rs','            egress.fetch_add(packet.len() as u64,Ordering::Relaxed);','            if fx_packet_trace::enabled() {if let Ok(h)=Header::parse(packet) {fx_packet_trace::emit(fx_packet_trace::stage::ADMIT,[0;16],[h.subscription,h.record_id,h.shard_index as u64],packet);}}\n            egress.fetch_add(packet.len() as u64,Ordering::Relaxed);')
    edit(out/'src/fxlink/client.rs','                if header.generation != 1 { continue; }','                if header.generation != 1 { continue; }\n                fx_packet_trace::emit(fx_packet_trace::stage::DISPATCH,[0;16],[header.subscription,header.record_id,header.shard_index as u64],&packet);')
    install_checkpoint_hooks(out,here,edit)
    # Ready-to-import private shaper, with the exact original scheduler/loss decisions.
    h=out/'packet-shaper';h.mkdir();shutil.copy2(a.shaper_source,h/'shaper.py');shutil.copy2(here/'packet_trace.py',h/'packet_trace.py')
    for name in ['capture_window.py','checkpoint_coordinator.py']:shutil.copy2(here/name,h/name)
    edit(h/'shaper.py','import helpers as h','import helpers as h\nfrom packet_trace import Recorder,SHAPER_SEEN,SHAPER_DROP,SHAPER_SENT,SHAPER_ERROR')
    edit(h/'shaper.py','        self.closing = False','        self.packet_trace=Recorder.from_env()\n        self.closing = False')
    edit(h/'shaper.py',"            self.error = 'proxy queue cap exceeded'","            self.packet_trace.record(SHAPER_ERROR,data,[0,int(direction=='down'),0])\n            self.error = 'proxy queue cap exceeded'")
    edit(h/'shaper.py','        self.serial += 1','        self.serial += 1\n        self.packet_trace.record(SHAPER_SEEN,data,[self.serial,int(direction==\'down\'),int(lost)+2*int(burst)])')
    edit(h/'shaper.py','            if lost:\n','            if lost:\n                self.packet_trace.record(SHAPER_DROP,data,[serial,int(d==\'down\'),int(burst)])\n')
    edit(h/'shaper.py','            elapsed = (at - queued) * 1000','            self.packet_trace.record(SHAPER_SENT,data,[serial,int(d==\'down\'),0])\n            elapsed = (at - queued) * 1000')
    with (h/'shaper.py').open('a') as f:f.write('\n    async def close(self):\n        try:\n            await super().close()\n        finally:\n            self.packet_trace.close()\n')
    (out/'instrumentation.json').write_text(json.dumps(dict(base_inputs={str(p.relative_to(repo)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [repo/'Cargo.toml',repo/'Cargo.lock',*sorted((repo/'src').rglob('*.rs')),*sorted((repo/'vendor').rglob('*.rs'))]},quinn_source=str(a.quinn_source),shaper_source_sha256=hashlib.sha256(a.shaper_source.read_bytes()).hexdigest()),indent=2)+'\n')
    print(out)
if __name__=='__main__':main()
