use crate::{
    error::DbResult,
    storage::record::{rowid::RowId, scan::constant::Constant},
};
mod constant;
mod table_scan;

pub(crate) use table_scan::TableScan;

pub trait Scan: RowImpl {
    fn get_int(&mut self, field_name: &str) -> DbResult<i32>;
    fn get_string(&mut self, field_name: &str) -> DbResult<String>;
    fn get_val(&mut self, field_name: &str) -> DbResult<Constant>;
    fn before_first(&mut self) -> DbResult<()>;
    fn has_field(&self, field_name: &str) -> bool;
    fn close(&mut self) -> DbResult<()>;
    fn next(&mut self) -> DbResult<bool>;
}

pub trait UpdateScan: Scan {
    fn set_int(&mut self, field_name: &str, val: i32) -> DbResult<()>;
    fn set_string(&mut self, field_name: &str, val: String) -> DbResult<()>;
    fn set_val(&mut self, field_name: &str, val: Constant) -> DbResult<()>;
    fn insert(&mut self) -> DbResult<()>;
    fn delete(&mut self) -> DbResult<()>;
}

pub trait RowImpl {
    fn get_row_id(&self) -> RowId;
    fn move_to_row_id(&mut self, row_id: RowId) -> DbResult<()>;
}
