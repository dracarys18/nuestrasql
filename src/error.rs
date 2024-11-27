#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("Failed to acquire lock, Lock timeout exceeded")]
    LockAborted,
    #[error("IO error occured {0:?}")]
    IoError(#[from] std::io::Error),
    #[error("Value from Option was invalid")]
    InvalidValue,
}

pub type DbResult<T> = Result<T, DbError>;
