//! A very dumb and specialized buffering layer for cfitsio S3 access.
//!
//! The design of this is *highly* tuned to the access patterns encountered when
//! cfitsio reads DASCH's compressed mosaic files. The pattern is as follows:
//!
//! 1. First, cfitsio reads the beginning of the file -- the empty-ish HDU 1
//! 2. Then, it reads the HDU 2 header, which is longer, and starts reading the
//!    beginning of the data, which indexes the compressed image data.
//! 3. Then, it reads the actual meat of the image data. As we progress through
//!    scanlines of the image, we mostly read this data, occasionally returning
//!    to the beginning of the HDU to get more indexing information.
//!
//! This suggests a three-segment buffer, with one segment for each region of
//! the file that we care about. The first segment can be a small buffer; the
//! second bigger; and the third should be biggest.

use anyhow::{bail, Result};
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use std::io::Write;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum BufferKind {
    A,
    B,
    C,
}

impl BufferKind {
    fn capacity(&self) -> usize {
        match self {
            BufferKind::A => 32768,
            BufferKind::B => 32768,
            BufferKind::C => 4194304,
        }
    }
}

#[derive(Debug)]
struct Buffer {
    pub data: Vec<u8>,
    pub start_file_offset: u64,
}

impl Buffer {
    fn new(kind: BufferKind) -> Self {
        Buffer {
            data: Vec::with_capacity(kind.capacity()),
            start_file_offset: 0,
        }
    }

    fn empty_or_overlaps(&self, offset: u64, nbytes: usize) -> bool {
        if self.data.is_empty() {
            return true;
        }

        (offset + nbytes as u64) < self.start_file_offset + self.data.len() as u64
    }

    async fn read_into<W: Write>(
        &mut self,
        get: GetObjectFluentBuilder,
        mut offset: u64,
        mut nbytes: usize,
        mut dest: W,
    ) -> Result<()> {
        // Can we service some or all of this request from what's already in the
        // buffer? We assume that reads basically move forward: if we try to
        // read a chunk that starts just before our currently available buffer,
        // we'll ignore everything that we have and refill the buffer.

        if offset >= self.start_file_offset {
            let i_start = (offset - self.start_file_offset) as usize;
            let i_end = usize::min(i_start + nbytes, self.data.len());

            if i_end > i_start {
                let n_available = i_end - i_start;
                dest.write_all(&self.data[i_start..i_end])?;
                nbytes -= n_available;
                offset += n_available as u64;
            }
        }

        if nbytes == 0 {
            return Ok(());
        }

        // Looks like we need to (re)fill the buffer in order to complete this
        // request.

        self.data.clear();
        self.start_file_offset = offset;

        //eprintln!("+s3buf {:?} fetching @ {}", self.kind, offset);

        // If we need more than our buffer fits, just grow the buffer.
        let end_byte = offset + usize::max(self.data.capacity(), nbytes) as u64 - 1;

        let mut result = get
            .range(format!("bytes={}-{}", offset, end_byte))
            .send()
            .await?;

        while let Some(bytes) = result.body.try_next().await? {
            self.data.extend_from_slice(&bytes);
        }

        if self.data.len() < nbytes {
            bail!("couldn't get enough S3 data to service FITS read request");
        }

        dest.write_all(&self.data[0..nbytes])?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct S3Buffer {
    buf_a: Buffer,
    buf_b: Buffer,
    buf_c: Buffer,
}

impl Default for S3Buffer {
    fn default() -> Self {
        S3Buffer {
            buf_a: Buffer::new(BufferKind::A),
            buf_b: Buffer::new(BufferKind::B),
            buf_c: Buffer::new(BufferKind::C),
        }
    }
}

impl S3Buffer {
    pub async fn read_into<W: Write>(
        &mut self,
        get: GetObjectFluentBuilder,
        offset: u64,
        nbytes: usize,
        dest: W,
    ) -> Result<()> {
        let buf = {
            if self.buf_a.empty_or_overlaps(offset, nbytes) {
                &mut self.buf_a
            } else if self.buf_b.empty_or_overlaps(offset, nbytes) {
                &mut self.buf_b
            } else if self.buf_c.empty_or_overlaps(offset, nbytes) {
                &mut self.buf_c
            } else if offset < self.buf_b.start_file_offset {
                &mut self.buf_a
            } else if offset < self.buf_c.start_file_offset {
                &mut self.buf_b
            } else {
                &mut self.buf_c
            }
        };

        buf.read_into(get, offset, nbytes, dest).await?;
        Ok(())
    }
}
