#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("Failed to acquire lock, Lock timeout exceeded")]
    LockAborted,
    #[error("IO error occurred {0:?}")]
    IoError(#[from] std::io::Error),
    #[error("Value from Option was invalid")]
    InvalidValue,
    #[error("Cannot find the field in the schema")]
    SchemaFieldNotFound,
    #[error("Offsets were available")]
    OffsetNotFound,
    #[error("Not slot were available")]
    SlotNotFound,
    #[error("Record page not set")]
    RecordPageNotSet,
    #[error("Table not found: {table_name}")]
    TableNotFound { table_name: String },
    #[error("View not found: {0}")]
    ViewNotFound(String),
    #[error("Unexpected Error")]
    Unexpected,
}

pub type DbResult<T> = Result<T, DbError>;
