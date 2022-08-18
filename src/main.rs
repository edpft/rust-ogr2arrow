use arrow::{
    self,
    array::{StringArray, ArrayRef, Float64Array, Int32Array, TimestampSecondArray},
    record_batch::RecordBatch,
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use chrono::{DateTime, Utc};
use std::{sync::Arc, iter::Map};
use rusqlite::{self, Connection};

fn get_data_type(sql_name: Option<&str>) -> DataType {
    sql_name
        .map(|name| match name {
            "TEXT" => DataType::Utf8,
            "DATETIME" => DataType::Timestamp(TimeUnit::Second, None),
            "DOUBLE" => DataType::Float64,
            "INTEGER" => DataType::Int32,
            &_ => todo!(),
        })
        .unwrap()
}

fn get_schema(connection: &Connection, table_name: &str) -> rusqlite::Result<Schema> {
    let sql = format!("SELECT * FROM {}", table_name);
    let statement = connection.prepare(&sql)?;

    let columns = statement.columns();

    let fields: Vec<Field> = columns
        .into_iter()
        .map(|column| Field::new(column.name(), get_data_type(column.decl_type()), false))
        .collect();

    Ok(Schema::new(fields))
}

fn main() -> rusqlite::Result<()> {
    let path = "Data/bdline_gb.gpkg";
    let connection = Connection::open(path)?;

    let schema = get_schema(&connection, "gpkg_contents")?;

    let names_and_types: Map<Iter<Field>>, Closure> = schema
        .fields()
        .iter()
        .map(|field| (field.name(), field.data_type()));

    let arrays: Vec<Arc<(dyn arrow::array::Array + 'static)>> = names_and_types
        .into_iter()
        .map(|(field_name, field_type)| {
            let sql = format!("SELECT {} FROM gpkg_contents", field_name);
            let mut statement = connection.prepare(&sql).unwrap();
            let mut rows = statement.query([]).unwrap();
            let array_ref = match field_type {
                DataType::Utf8 => {
                    let mut values: Vec<String> = Vec::new();
                    while let Some(row) = rows.next().unwrap() {
                        values.push(row.get(0).unwrap())
                    }
                    let data = StringArray::from_iter_values(values);
                    Arc::new(data) as ArrayRef
                }
                DataType::Float64 => {
                    let mut values: Vec<f64> = Vec::new();
                    while let Some(row) = rows.next().unwrap() {
                        values.push(row.get(0).unwrap())
                    }
                    let data = Float64Array::from_iter_values(values);
                    Arc::new(data) as ArrayRef
                }
                DataType::Int32 => {
                    let mut values: Vec<i32> = Vec::new();
                    while let Some(row) = rows.next().unwrap() {
                        values.push(row.get(0).unwrap())
                    }
                    let data = Int32Array::from_iter_values(values);
                    Arc::new(data) as ArrayRef
                }
                DataType::Timestamp(TimeUnit::Second, None) => {
                    let mut values: Vec<i64> = Vec::new();
                    while let Some(row) = rows.next().unwrap() {
                        let date_time: DateTime<Utc> = row.get(0).unwrap();
                        values.push(date_time.timestamp())
                    }
                    let data = TimestampSecondArray::from_iter_values(values);
                    Arc::new(data) as ArrayRef
                }
                _ => todo!(),
            };
            array_ref
        })
        .collect();

    let batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();

    dbg!(batch);

    Ok(())
}
