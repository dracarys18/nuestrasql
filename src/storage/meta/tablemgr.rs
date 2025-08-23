use crate::{
    consts::GULAG_MSG,
    error::{DbError, DbResult},
    storage::record::{
        layout::Layout,
        scan::{Scan, TableScan, UpdateScan},
        schema::{self, Schema},
    },
    storage::tx::Transactions,
};

use std::{collections::HashMap, num::NonZeroUsize};

pub(super) const MAX_NAME_LEN: usize = 64;

pub(crate) struct TableManager {
    table_cat: Layout,
    field_cat: Layout,
}

impl TableManager {
    pub fn new(is_new: bool, tx: Transactions) -> DbResult<Self> {
        let mut tcat_schema = Schema::new();
        tcat_schema.add_string_field("table_name".to_string(), MAX_NAME_LEN);
        tcat_schema.add_int_field("slotsize".to_string());

        let mut fcat_schema = Schema::new();
        fcat_schema.add_string_field("table_name".to_string(), MAX_NAME_LEN);
        fcat_schema.add_string_field("field_name".to_string(), MAX_NAME_LEN);
        fcat_schema.add_int_field("field_type".to_string());
        fcat_schema.add_int_field("field_length".to_string());
        fcat_schema.add_int_field("field_offset".to_string());

        let table_cat = Layout::new(tcat_schema.clone())?;
        let field_cat = Layout::new(fcat_schema.clone())?;

        let this = Self {
            table_cat: table_cat.clone(),
            field_cat: field_cat.clone(),
        };

        if is_new {
            this.create_table("table_cat".to_string(), table_cat, tx.clone())?;
            this.create_table("field_cat".to_string(), field_cat, tx)?;
        }

        Ok(this)
    }

    /// Creates the table by saving the schema information on it in table_cat and field_cat file
    /// respectively.
    pub fn create_table(
        &self,
        table_name: String,
        layout: Layout,
        tx: Transactions,
    ) -> DbResult<()> {
        let mut tcat = TableScan::new(tx.clone(), "table_cat".to_string(), layout.clone())?;
        tcat.insert()?;

        tcat.set_string("table_name", table_name.clone())?;
        tcat.set_int("slotsize", layout.slot_size().get() as i32)?;
        tcat.close()?;

        let mut field_cat = TableScan::new(tx, "field_cat".to_string(), layout.clone())?;

        for field_name in layout.schema().fields() {
            field_cat.insert()?;
            field_cat.set_string("table_name", table_name.clone())?;
            field_cat.set_string("field_name", field_name.clone())?;
            field_cat.set_int("field_type", layout.schema().typ(field_name)? as i32)?;
            field_cat.set_int("field_length", layout.schema().length(field_name)? as i32)?;
            field_cat.set_int("field_offset", layout.offset(field_name)? as i32)?;
        }
        field_cat.close()?;

        Ok(())
    }

    /// Get the layout of a table, it uses already saved table_cata and field_cat meta
    /// tables to fetch the information about the schema
    pub fn get_layout(&self, table_name: &str, tx: Transactions) -> DbResult<Layout> {
        let mut size = -1;

        let mut tcat = TableScan::new(tx.clone(), "table_cat".to_string(), self.table_cat.clone())?;

        // Move until the tablescan moves to the place where the meta of the table is saved
        // in table_cat table file
        while tcat.next()? {
            if tcat.get_string("table_name")?.eq(table_name) {
                size = tcat.get_int("slotsize")?;
                break;
            }
        }

        // This should ideally not happen. i.e a slot size for a table should never be zero
        if size <= 0 {
            return Err(DbError::TableNotFound {
                table_name: table_name.to_string(),
            });
        }

        tcat.close()?;
        let size = NonZeroUsize::new(size as usize).expect(GULAG_MSG);

        let mut schema = Schema::new();
        let mut offsets = HashMap::new();

        let mut fcat = TableScan::new(tx, "field_cat".to_string(), self.field_cat.clone())?;
        while fcat.next()? {
            if fcat.get_string("table_name")?.eq(table_name) {
                let field_name = fcat.get_string("field_name")?;
                let field_type = fcat.get_int("field_type")? as u32;
                let field_length = fcat.get_int("field_length")? as usize;
                let field_offset = fcat.get_int("field_offset")? as usize;

                offsets.insert(field_name.clone(), field_offset);
                schema.add_field(
                    field_name,
                    schema::FieldType::from(field_type),
                    field_length,
                );
            }
        }
        fcat.close()?;

        Ok(Layout::new_with_data(schema, offsets, size))
    }
}
