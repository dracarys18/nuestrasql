use crate::error::{DbError, DbResult};
use std::collections::HashMap;

#[repr(u32)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum FieldType {
    Integer,
    Varchar,
}

/// Maintains the information about fields, particularly type and length of a field
#[derive(Clone)]
pub(crate) struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            info: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, field_name: String, typ: FieldType, length: usize) {
        self.fields.push(field_name.clone());
        self.info.insert(field_name, FieldInfo::new(typ, length));
    }

    pub fn add_int_field(&mut self, field_name: String) {
        self.add_field(field_name, FieldType::Integer, 0);
    }

    pub fn add_string_field(&mut self, field_name: String, length: usize) {
        self.add_field(field_name, FieldType::Varchar, length);
    }

    pub fn add(&mut self, field_name: String, schema: Schema) -> DbResult<()> {
        let len = schema.length(&field_name)?;
        let typ = schema.typ(&field_name)?;

        self.add_field(field_name, typ, len);
        Ok(())
    }

    pub fn typ(&self, field_name: &str) -> DbResult<FieldType> {
        self.info
            .get(field_name)
            .ok_or(DbError::SchemaFieldNotFound)
            .map(|f| f.typ)
    }

    pub fn length(&self, field_name: &String) -> DbResult<usize> {
        self.info
            .get(field_name)
            .ok_or(DbError::SchemaFieldNotFound)
            .map(|field| field.length)
    }

    pub fn add_all(&mut self, schema: Schema) -> DbResult<()> {
        for field_name in schema.fields() {
            self.add(field_name.clone(), schema.clone())?;
        }
        Ok(())
    }

    pub fn has_field(&self, field_name: &str) -> bool {
        self.fields.contains(&field_name.to_string())
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }
}

#[derive(Clone)]
pub(crate) struct FieldInfo {
    typ: FieldType,
    length: usize,
}

impl FieldInfo {
    fn new(typ: FieldType, length: usize) -> Self {
        Self { typ, length }
    }
}
