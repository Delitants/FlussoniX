# Owned codec fixtures

Generated on 2026-10-03 UTC with independently installed FFmpeg 7.1.1. No Flussonic media, code, binaries or credentials are included.

HEVC: `ffmpeg -f lavfi -i testsrc2=size=128x96:rate=25 -frames:v 12 -an -c:v libx265 -preset ultrafast -x265-params pools=1:frame-threads=1:keyint=6:bframes=2:log-level=error hevc.mp4`.
`ffprobe -show_streams -show_packets -show_data -of json hevc.mp4` supplies the unchanged hvcC bytes, each unchanged length-prefixed packet and timing. The MP4 timebase is 1/12800; native test timestamps shift the negative initial DTS into a nonnegative timeline. This is an 8-bit Main fixture with B-frames, not a Main10 qualification.

MPEG Layer II: five frames of a 440 Hz sine at48000 Hz, `-c:a mp2 -b:a 192k -f mp2`. Layer III: five frames of a660 Hz sine at22050 Hz, `-c:a libmp3lame -b:a 64k -f mp3`. The first demuxed packet of each is retained, without container metadata. These mono packets demonstrate MPEG1 Layer II's1152 samples and MPEG2 Layer III's576 samples. Additional synthetic headers exercise the bounded parser's format combinations; they do not claim decoded profile coverage.
