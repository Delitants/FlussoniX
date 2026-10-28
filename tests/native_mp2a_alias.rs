use flussonix::{
    m4f::{self, Frame},
    m4s::{self, Decoder, Event, Track},
    wire,
};

const MP2: &[u8] = include_bytes!("fixtures/codecs/mp2.bin");

fn track() -> Track {
    Track {
        id: 1,
        codec: "m2a".into(),
        config: vec![],
    }
}

fn frame() -> Frame {
    Frame {
        track_id: 1,
        dts: 90_000,
        pts_offset: 0,
        key: true,
        body: MP2.to_vec(),
    }
}

fn vendor_m4f() -> Vec<u8> {
    let canonical = m4f::pack(&[track()], &[frame()], 2_160).unwrap();
    let root = m4s::boxes(&canonical).unwrap();
    let mut moov = Vec::new();
    for (kind, body) in m4s::boxes(root[0].1).unwrap() {
        if kind != b"trak" {
            moov.extend(m4s::atom(kind.try_into().unwrap(), body));
            continue;
        }
        let mut fields = Vec::new();
        for (field, value) in m4s::boxes(body).unwrap() {
            if field == b"hdlr" {
                assert!(value.ends_with(b"m2a\0"));
                let mut vendor = value[..value.len() - 4].to_vec();
                vendor.extend_from_slice(b"mp2a\0");
                fields.extend(m4s::atom(b"hdlr", &vendor));
            } else {
                fields.extend(m4s::atom(field.try_into().unwrap(), value));
            }
        }
        moov.extend(m4s::atom(b"trak", &fields));
    }
    [m4s::atom(b"moov", &moov), m4s::atom(b"mdat", root[1].1)].concat()
}

#[test]
fn flussonic_mp2a_m4f_unpacks_as_mpeg_layer_ii() {
    let vendor = vendor_m4f();
    let (tracks, frames) = m4f::unpack(&vendor).unwrap();
    assert_eq!(tracks, vec![track()]);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].body, MP2);
    assert_eq!(frames[0].dts, 90_000);
}

#[test]
fn flussonic_mp2a_m4s_preserves_wire_records_and_normalizes_audio_track() {
    let mut handler = vec![0; 4];
    handler.extend_from_slice(&1u32.to_be_bytes());
    handler.extend_from_slice(b"sounmp2a\0");
    let metadata = m4s::atom(
        b"MDin",
        &m4s::atom(
            b"trak",
            &[m4s::atom(b"hdlr", &handler), m4s::atom(b"cnfg", &[0; 4])].concat(),
        ),
    );
    let mut info_wire = (metadata.len() as u32).to_be_bytes().to_vec();
    info_wire.extend(metadata);

    let mut frame_wire = wire::encode_frame(&track(), &frame()).unwrap();
    let tag = frame_wire
        .windows(4)
        .position(|bytes| bytes == b" m2a")
        .unwrap();
    frame_wire[tag..tag + 4].copy_from_slice(b"mp2a");

    let mut decoder = Decoder::default();
    let events = decoder
        .push(&[info_wire.as_slice(), frame_wire.as_slice()].concat())
        .unwrap();
    assert_eq!(events.len(), 2);
    match &events[0] {
        Event::Info { tracks, wire } => {
            assert_eq!(tracks, &vec![track()]);
            assert_eq!(wire.as_ref(), info_wire);
        }
        _ => panic!("expected native metadata"),
    }
    match &events[1] {
        Event::Frame { body, wire, .. } => {
            assert_eq!(body, MP2);
            assert_eq!(wire.as_ref(), frame_wire);
        }
        _ => panic!("expected native MPEG Layer II frame"),
    }
}
