use crate::disk::page::Page;

use crate::error::DbResult;
use crate::tx::{
    recovery::{
        checkpoint::*, commit_log::*, rollback::*, set_int::*, set_string::*, start_log::*,
    },
    Transactions,
};

#[repr(i32)]
#[derive(Eq, PartialEq)]
pub enum LogOperation {
    Checkpoint = 1,
    Start = 2,
    Commit = 3,
    Rollback = 4,
    SetInt = 5,
    SetString = 6,
}

impl LogOperation {
    fn from_i32(val: i32) -> Self {
        unsafe { std::mem::transmute(val) }
    }
}

pub trait RecordLog {
    fn op(&self) -> LogOperation;
    fn tx_number(&self) -> i32;
    fn undo(&self, tx: &mut Transactions) -> DbResult<()>;
}

pub fn create_log_record(bytes: Vec<u8>) -> Box<dyn RecordLog> {
    let mut page = Page::new_with_data(bytes);
    match LogOperation::from_i32(page.get_int(0)) {
        LogOperation::Checkpoint => Box::new(Checkpoint::new()),
        LogOperation::Rollback => Box::new(Rollback::new(page)),
        LogOperation::Commit => Box::new(CommitLog::new(page)),
        LogOperation::SetInt => Box::new(SetIntRecord::new(page)),
        LogOperation::SetString => Box::new(SetStringRecord::new(page)),
        LogOperation::Start => Box::new(StartLog::new(page)),
    }
}
