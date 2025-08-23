use crate::{
    error::{DbError, DbResult},
    storage::meta::tablemgr::{TableManager, MAX_NAME_LEN},
    storage::record::{
        layout::Layout,
        scan::{Scan, TableScan, UpdateScan},
        schema::Schema,
    },
    storage::tx::Transactions,
};

const MAX_VIEDEF: usize = 100;

/// ViewMgr is responsible for managing views in the database.
pub(crate) struct ViewMgr {
    tm: TableManager,
}

impl ViewMgr {
    /// Creates a new ViewMgr instance. If `is_new` is true, it initializes the view catalog table.
    ///
    /// Here view_def the query you run to create view
    pub(crate) fn new(is_new: bool, tm: TableManager, tx: Transactions) -> DbResult<Self> {
        let this = Self { tm };

        if is_new {
            let mut schema = Schema::new();

            schema.add_string_field(String::from("viewname"), MAX_NAME_LEN);
            schema.add_string_field(String::from("viewdef"), MAX_VIEDEF);

            let layout = Layout::new(schema)?;

            this.tm.create_table("view_cat".to_string(), layout, tx)?
        }

        Ok(this)
    }

    /// Creates a new view in the view catalog.
    pub(crate) fn create_view(
        &self,
        view_name: String,
        view_def: String,
        tx: Transactions,
    ) -> DbResult<()> {
        let layout = self.tm.get_layout("view_cat", tx.clone())?;
        let mut ts = TableScan::new(tx.clone(), String::from("view_cat"), layout)?;

        ts.set_string("view_name", view_name)?;
        ts.set_string("view_def", view_def)?;

        ts.close()?;

        Ok(())
    }

    /// Retrieves the definition of a view by its name.
    pub fn get_view_def(&self, view_name: String, tx: Transactions) -> DbResult<String> {
        let layout = self.tm.get_layout("view_cat", tx.clone())?;
        let mut ts = TableScan::new(tx, String::from("view_cat"), layout).unwrap();

        while ts.next()? {
            if ts.get_string("view_name")? == view_name {
                return ts.get_string("view_def");
            }
        }

        Err(DbError::ViewNotFound(view_name))
    }
}
