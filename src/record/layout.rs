use super::schema::{FieldType, Schema};
use crate::{
    consts::INTEGER_BYTES,
    disk::page::Page,
    error::{DbError, DbResult},
};

use std::collections::HashMap;

pub(crate) struct Layout {
    schema: Schema,
    offsets: HashMap<String, usize>,
    slot_size: usize,
}

impl Layout {
    pub fn new(schema: Schema) -> DbResult<Self> {
        let mut pos = INTEGER_BYTES;
        let offsets = schema
            .fields()
            .iter()
            .map(|field_name| {
                let offset = pos;
                pos += Self::length_in_bytes(&schema, field_name)?;
                Ok::<_, DbError>((field_name.clone(), offset))
            })
            .collect::<DbResult<HashMap<String, usize>>>()?;

        Ok(Self {
            offsets,
            schema,
            slot_size: pos,
        })
    }

    pub fn new_with_data(
        schema: Schema,
        offsets: HashMap<String, usize>,
        slot_size: usize,
    ) -> Self {
        Self {
            slot_size,
            offsets,
            schema,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn offset(&self, field_name: &String) -> DbResult<usize> {
        self.offsets
            .get(field_name)
            .copied()
            .ok_or(DbError::OffsetNotFound)
    }

    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    pub fn length_in_bytes(schema: &Schema, field_name: &String) -> DbResult<usize> {
        let field_type = schema.typ(field_name)?;

        Ok(if field_type.eq(&FieldType::Integer) {
            INTEGER_BYTES
        } else {
            Page::max_len(schema.length(field_name)?)
        })
    }
}
