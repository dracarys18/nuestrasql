pub(super) struct SimpleBytesCursor {
    pos: usize,
    data: Vec<u8>,
}

impl SimpleBytesCursor {
    pub(super) fn with_capacity(size: usize) -> Self {
        let data = vec![0; size];

        Self { pos: 0, data }
    }

    pub(super) fn get_i32(&mut self) -> i32 {
        let mut buf: [u8; 4] = [0; 4];
        buf.copy_from_slice(&self.data[self.pos..self.pos + 4]);

        self.pos += 4;

        i32::from_be_bytes(buf)
    }

    pub(super) fn set_i32(&mut self, val: i32) {
        let bytes = val.to_be_bytes();

        let buf = &mut self.data[self.pos..self.pos + 4];

        buf.copy_from_slice(&bytes);
        self.pos += 4;
    }

    pub(super) fn set_slice(&mut self, data: &[u8]) {
        let dest = &mut self.data[self.pos..self.pos + data.len()];

        dest.copy_from_slice(data);
        self.pos += data.len()
    }

    pub(super) fn set_position(&mut self, pos: usize) {
        self.pos = pos;
    }
}

impl AsMut<[u8]> for SimpleBytesCursor {
    fn as_mut(&mut self) -> &mut [u8] {
        self.pos = 0;
        &mut self.data
    }
}

impl AsRef<[u8]> for SimpleBytesCursor {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl From<&[u8]> for SimpleBytesCursor {
    fn from(value: &[u8]) -> Self {
        Self {
            pos: 0,
            data: value.to_vec(),
        }
    }
}

impl std::ops::Index<std::ops::Range<usize>> for SimpleBytesCursor {
    type Output = [u8];
    fn index(&self, index: std::ops::Range<usize>) -> &Self::Output {
        self.data.index(index)
    }
}
