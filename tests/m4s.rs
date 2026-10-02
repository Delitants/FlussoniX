use flussonix::m4s::{Decoder, Event, atom};
#[test]
fn split_packets_and_avc_aac_headers_are_decoded_without_guessing_fmp4() {
    let mut handler = vec![0, 0, 0, 0];
    handler.extend_from_slice(&1u32.to_be_bytes());
    handler.extend_from_slice(b"videh264\0");
    let mut cfg = vec![0, 0, 0, 0];
    cfg.extend_from_slice(&[1, 100, 0, 40, 255, 225, 0, 2, 103, 1, 1, 0, 2, 104, 1]);
    let track = atom(
        b"trak",
        &[atom(b"hdlr", &handler), atom(b"cnfg", &cfg)].concat(),
    );
    let info = atom(b"MDin", &track);
    let packet = [(info.len() as u32).to_be_bytes().to_vec(), info].concat();
    let mut d = Decoder::default();
    assert!(d.push(&packet[..7]).unwrap().is_empty());
    let events = d.push(&packet[7..]).unwrap();
    match &events[0] {
        Event::Info { tracks, .. } => {
            assert_eq!(tracks[0].id, 1);
            assert_eq!(tracks[0].codec, "h264");
            assert_eq!(tracks[0].config[0], 1)
        }
        _ => panic!("expected media info"),
    }
    let mut fh = 1u32.to_be_bytes().to_vec();
    fh.extend_from_slice(&[1, 2, 1, 128]);
    fh.extend_from_slice(b"h264");
    fh.extend_from_slice(&90000u64.to_be_bytes());
    fh.extend_from_slice(&(-3600i64).to_be_bytes());
    fh.extend_from_slice(&0u32.to_be_bytes());
    let f = atom(
        b"FRam",
        &[atom(b"fhdr", &fh), atom(b"body", &[0, 0, 0, 2, 101, 1])].concat(),
    );
    let packet = [(f.len() as u32).to_be_bytes().to_vec(), f].concat();
    match &d.push(&packet).unwrap()[0] {
        Event::Frame {
            track_id,
            dts,
            pts_offset,
            key,
            body,
            ..
        } => {
            assert_eq!(*track_id, 1);
            assert_eq!(*dts, 90000);
            assert_eq!(*pts_offset, -3600);
            assert!(*key);
            assert_eq!(body.len(), 6)
        }
        _ => panic!("expected frame"),
    }
    assert!(
        Decoder::default()
            .push(&0xffff_ffffu32.to_be_bytes())
            .is_err()
    );
}
#[test]
fn m4f_roundtrip_preserves_samples_timestamps_and_rejects_truncation() {
    use flussonix::m4f::{Frame, pack, unpack};
    let track = flussonix::m4s::Track {
        id: 1,
        codec: "h264".into(),
        config: vec![1, 100, 0, 40],
    };
    let frames = vec![
        Frame {
            track_id: 1,
            dts: 90000,
            pts_offset: 3600,
            key: true,
            body: vec![0, 0, 0, 1, 101],
        },
        Frame {
            track_id: 1,
            dts: 93600,
            pts_offset: 0,
            key: false,
            body: vec![0, 0, 0, 1, 65],
        },
    ];
    let b = pack(&[track], &frames, 7200).unwrap();
    let (tracks, decoded) = unpack(&b).unwrap();
    assert_eq!(tracks[0].codec, "h264");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].dts, 90000);
    assert_eq!(decoded[0].pts_offset, 3600);
    assert_eq!(decoded[1].body, frames[1].body);
    assert!(unpack(&b[..b.len() - 1]).is_err());
}

#[test]
fn malformed_m4f_tracks_and_empty_compositions_return_errors_without_amplification() {
    use flussonix::{
        m4f::{Frame, pack, unpack},
        m4s::{Track, boxes},
    };
    let track = Track {
        id: 1,
        codec: "h264".into(),
        config: vec![1, 100, 0, 40],
    };
    let frame = Frame {
        track_id: 1,
        dts: 0,
        pts_offset: 0,
        key: true,
        body: vec![0; 1024],
    };
    let input = pack(&[track], &[frame], 3600).unwrap();
    let root = boxes(&input).unwrap();
    let moov = boxes(root[0].1).unwrap();
    let segm = atom(b"segm", moov[0].1);
    let trak = atom(b"trak", moov[1].1);
    let many = [segm.clone(), trak.repeat(100)].concat();
    assert!(unpack(&[atom(b"moov", &many), atom(b"mdat", root[1].1)].concat()).is_err());
    let empty_ctts = [moov[1].1.to_vec(), atom(b"ctts", &[])].concat();
    let malformed = [
        atom(b"moov", &[segm, atom(b"trak", &empty_ctts)].concat()),
        atom(b"mdat", root[1].1),
    ]
    .concat();
    assert!(unpack(&malformed).is_err());
}
