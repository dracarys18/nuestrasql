#[derive(Eq, PartialEq, Hash, Clone)]
pub struct Block {
    filename: String,
    num: u64,
}

impl Block {
    pub fn new(filename: String, num: u64) -> Self {
        Self { filename, num }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn num(&self) -> u64 {
        self.num
    }
}

impl std::fmt::Display for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[file {} block {}]", self.filename, self.num)
    }
}
