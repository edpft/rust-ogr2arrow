use arrow::{
    self,
    array::{ArrayRef, Float64Array, Int32Array, StringArray, TimestampSecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use chrono::{DateTime, Utc};
use fallible_iterator::FallibleIterator;
use rusqlite::{self, Connection};
use std::{iter::Iterator, sync::Arc};

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

fn get_columns(schema: &Schema, connection: &Connection) -> Vec<Arc<(dyn arrow::array::Array)>> {
    let names_and_types = schema
        .fields()
        .iter()
        .map(|field| (field.name(), field.data_type()));

    names_and_types
        .map(|(field_name, field_type)| {
            let sql = format!("SELECT {} FROM gpkg_contents", field_name);
            let mut statement = connection
                .prepare(&sql)
                .expect("Failed to prepare statment.");
            let rows = statement.query([]).expect("Failed to execute query.");
            let array_ref = match field_type {
                DataType::Utf8 => {
                    let values: rusqlite::Result<Vec<String>> = rows
                        .map(|row| {
                            let value: rusqlite::Result<String> = row.get(0);
                            value
                        })
                        .collect();
                    let data = StringArray::from_iter_values(values.unwrap());
                    Arc::new(data) as ArrayRef
                }
                DataType::Float64 => {
                    let values: rusqlite::Result<Vec<f64>> = rows
                        .map(|row| {
                            let value: rusqlite::Result<f64> = row.get(0);
                            value
                        })
                        .collect();
                    let data = Float64Array::from_iter_values(values.unwrap());
                    Arc::new(data) as ArrayRef
                }
                DataType::Int32 => {
                    let values: rusqlite::Result<Vec<i32>> = rows
                        .map(|row| {
                            let value: rusqlite::Result<i32> = row.get(0);
                            value
                        })
                        .collect();
                    let data = Int32Array::from_iter_values(values.unwrap());
                    Arc::new(data) as ArrayRef
                }
                DataType::Timestamp(TimeUnit::Second, None) => {
                    let values: rusqlite::Result<Vec<i64>> = rows
                        .map(|row| {
                            let value: DateTime<Utc> = row.get(0)?;
                            Ok(value.timestamp())
                        })
                        .collect();
                    let data = TimestampSecondArray::from_iter_values(values.unwrap());
                    Arc::new(data) as ArrayRef
                }
                _ => unimplemented!(),
            };
            array_ref
        })
        .collect()
}

fn main() -> rusqlite::Result<()> {
    let path = "Data/bdline_gb.gpkg";
    let connection = Connection::open(path)?;

    let schema = get_schema(&connection, "gpkg_contents")?;

    let columns = get_columns(&schema, &connection);

    let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();

    dbg!(batch);

    Ok(())
}
