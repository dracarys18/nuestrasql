use super::cursor::SimpleBytesCursor;

const MAX_BYTES_PER_CHAR: usize = 1;

pub struct Page {
    blob: SimpleBytesCursor,
    size: usize,
}

impl Page {
    pub fn new(block_size: u64) -> Self {
        Self {
            blob: SimpleBytesCursor::with_capacity(block_size as usize),
            size: 0,
        }
    }

    pub fn new_with_data(data: Vec<u8>) -> Self {
        let size = data.len();
        Self {
            blob: data.into(),
            size,
        }
    }

    pub fn get_int(&mut self, offset: usize) -> i32 {
        self.blob.set_position(offset);

        self.blob.get_i32()
    }

    pub fn set_int(&mut self, offset: usize, val: i32) {
        self.blob.set_position(offset);

        self.blob.set_i32(val);
        self.size += 4;
    }

    pub fn get_bytes(&mut self, offset: usize) -> &[u8] {
        self.blob.set_position(offset);

        let length = self.blob.get_i32();
        &self.blob[offset + 4..offset + 4 + length as usize]
    }

    pub fn set_bytes(&mut self, offset: usize, data: &[u8]) {
        self.blob.set_position(offset);
        self.blob.set_i32(data.len() as i32);
        self.blob.set_slice(data);

        self.size += 4 + data.len();
    }

    pub fn get_string(&mut self, offset: usize) -> String {
        let bytes = self.get_bytes(offset);

        // Panic Safety: This will not fail as we wont write a non utf8 character
        String::from_utf8(bytes.to_vec()).expect("Failed to decode utf8 characters")
    }

    pub fn set_string(&mut self, offset: usize, data: String) {
        let bytes = data.into_bytes();

        self.set_bytes(offset, &bytes);
    }

    pub fn max_len(len: usize) -> usize {
        4 + len * MAX_BYTES_PER_CHAR
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn contents(&mut self) -> &mut [u8] {
        self.blob.as_mut()
    }
}
