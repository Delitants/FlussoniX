//! Bounded DVB palette defaults and interlaced pixel-code drawing.
use super::{Color, PIXELS, Region, Result};
pub(super) fn default_clut() -> Box<[[Color; 256]; 3]> {
    let mut out = Box::new([[[0, 0]; 256]; 3]);
    out[0][1] = [255, 255];
    out[0][2] = [0, 255];
    out[0][3] = [128, 255];
    for i in 1..256 {
        let (r, g, b, a) = if i < 8 {
            (
                (i & 1) * 255,
                ((i >> 1) & 1) * 255,
                ((i >> 2) & 1) * 255,
                63,
            )
        } else {
            let mask = i & 0x88;
            let (low, high, base, alpha) = match mask {
                0 => (85, 170, 0, 255),
                8 => (85, 170, 0, 127),
                128 => (43, 85, 127, 255),
                _ => (43, 85, 0, 255),
            };
            (
                (i & 1) * low + ((i >> 4) & 1) * high + base,
                ((i >> 1) & 1) * low + ((i >> 5) & 1) * high + base,
                ((i >> 2) & 1) * low + ((i >> 6) & 1) * high + base,
                alpha,
            )
        };
        out[2][i] = [((r * 77 + g * 150 + b * 29 + 128) / 256) as u8, a as u8];
    }
    for i in 1..16 {
        let v = if i < 8 { 255 } else { 127 };
        let (r, g, b) = ((i & 1) * v, ((i >> 1) & 1) * v, ((i >> 2) & 1) * v);
        out[1][i] = [((r * 77 + g * 150 + b * 29 + 128) / 256) as u8, 255];
    }
    out
}
struct Bits<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl Bits<'_> {
    fn take(&mut self, n: usize) -> Result<u8> {
        if self.at + n > self.bytes.len() * 8 {
            return Err("dvb_pixel_string");
        }
        let mut value = 0;
        for _ in 0..n {
            value = (value << 1) | ((self.bytes[self.at / 8] >> (7 - self.at % 8)) & 1);
            self.at += 1;
        }
        Ok(value)
    }
    fn align(&mut self) {
        self.at = self.at.div_ceil(8) * 8;
    }
}
pub(super) fn paint(
    r: &mut Region,
    x: usize,
    y: usize,
    field: &[u8],
    nonmod: bool,
    budget: &mut usize,
) -> Result<()> {
    let (mut x, mut y) = (x, y);
    let origin = x;
    let mut bits = Bits {
        bytes: field,
        at: 0,
    };
    let (mut map24, mut map28, mut map48) = (
        [0, 7, 8, 15],
        [0, 119, 136, 255],
        std::array::from_fn::<_, 16, _>(|i| (i * 17) as u8),
    );
    while bits.at < field.len() * 8 {
        let kind = bits.take(8)?;
        match kind {
            0x20 => {
                for v in &mut map24 {
                    *v = bits.take(4)?;
                }
            }
            0x21 => {
                for v in &mut map28 {
                    *v = bits.take(8)?;
                }
            }
            0x22 => {
                for v in &mut map48 {
                    *v = bits.take(8)?;
                }
            }
            0xf0 => {
                x = origin;
                y = y.checked_add(2).ok_or("dvb_pixel_position")?;
            }
            0x10..=0x12 => {
                let bpp = 1 << (kind - 0x0f);
                if bpp > 1 << r.depth {
                    return Err("dvb_pixel_depth_unsupported");
                }
                loop {
                    let literal = bits.take(bpp as usize)?;
                    let (n, c) = if literal != 0 {
                        (1, literal)
                    } else {
                        match kind {
                            0x10 => {
                                if bits.take(1)? != 0 {
                                    (usize::from(bits.take(3)?) + 3, bits.take(2)?)
                                } else if bits.take(1)? != 0 {
                                    (1, 0)
                                } else {
                                    match bits.take(2)? {
                                        0 => (0, 0),
                                        1 => (2, 0),
                                        2 => (usize::from(bits.take(4)?) + 12, bits.take(2)?),
                                        _ => (usize::from(bits.take(8)?) + 29, bits.take(2)?),
                                    }
                                }
                            }
                            0x11 => {
                                if bits.take(1)? == 0 {
                                    let n = bits.take(3)?;
                                    (if n == 0 { 0 } else { usize::from(n) + 2 }, 0)
                                } else if bits.take(1)? == 0 {
                                    (usize::from(bits.take(2)?) + 4, bits.take(4)?)
                                } else {
                                    match bits.take(2)? {
                                        0 => (1, 0),
                                        1 => (2, 0),
                                        2 => (usize::from(bits.take(4)?) + 9, bits.take(4)?),
                                        _ => (usize::from(bits.take(8)?) + 25, bits.take(4)?),
                                    }
                                }
                            }
                            _ => {
                                let colored = bits.take(1)? != 0;
                                let n = usize::from(bits.take(7)?);
                                (n, if colored { bits.take(8)? } else { 0 })
                            }
                        }
                    };
                    if n == 0 {
                        bits.align();
                        break;
                    }
                    let end = x
                        .checked_add(n)
                        .filter(|end| *end <= r.width)
                        .ok_or("dvb_pixel_position")?;
                    if y >= r.height {
                        return Err("dvb_pixel_position");
                    }
                    *budget += n;
                    if *budget > 4 * PIXELS {
                        return Err("dvb_render_limit");
                    }
                    let mapped = match (kind, r.depth) {
                        (0x10, 2) => map24[usize::from(c)],
                        (0x10, 3) => map28[usize::from(c)],
                        (0x11, 3) => map48[usize::from(c)],
                        _ => c,
                    };
                    if !nonmod || c != 1 {
                        r.pixels[y * r.width + x..y * r.width + end].fill(mapped);
                    }
                    x = end;
                }
            }
            _ => return Err("dvb_pixel_type_unsupported"),
        }
    }
    Ok(())
}
