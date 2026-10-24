"""Install failure hooks only into the private source copy."""
def install_checkpoint_hooks(out,here,edit):
 receiver=out/'src/fxlink/receiver.rs';client=out/'src/fxlink/client.rs'
 # All typed tick/announcement/decoder expiry paths pass this exact identity.
 edit(receiver,'            self.stats.expired_records += 1;','            fx_packet_trace::failure(self.subscription,record_id,1);\n            self.stats.expired_records += 1;')
 # The conservative outer fence may expire without a known exact record.
 # Reason2 labels delivered as a head candidate, never an exact culprit.
 edit(client,'                        return Err("FXLink record deadline".into());','                        fx_packet_trace::failure(id,delivered,2);\n                        return Err("FXLink record deadline".into());')
 edit(client,'                        if !state.current() || now()>=deadline {return Err("FXLink retired or expired".into());}',
      '                        if !state.current() || now()>=deadline {if state.current() {fx_packet_trace::failure(id,delivered.saturating_sub(1),3);}return Err("FXLink retired or expired".into());}')
 test=out/'src/fxlink/startup_tests.rs'
 test.write_text(test.read_text()+'\n'+(here/'expiry_test.rs').read_text())
