use arrow::{
    self,
    array::{ArrayRef, Float64Array, Int32Array, StringArray, TimestampSecondArray, BooleanArray, PrimitiveArray},
    datatypes::{DataType, Field, Schema, TimeUnit, Int8Type},
    record_batch::RecordBatch,
};
use chrono::{DateTime, Utc};
use fallible_iterator::FallibleIterator;
use rusqlite::{self, Connection, named_params};
use std::{iter::Iterator, sync::Arc};

fn get_data_type(sql_name: Option<&str>) -> DataType {
    sql_name
        .map(|name| match name {
            "BOOLEAN" => DataType::Boolean,
            "TEXT" => DataType::Utf8,
            "DATETIME" => DataType::Timestamp(TimeUnit::Second, None),
            "DOUBLE" => DataType::Float64,
            "INTEGER" => DataType::Int32,
            &_ => todo!(),
        })
        .unwrap()
}

fn get_schema(connection: &Connection, layer: &str) -> rusqlite::Result<Schema> {
    let sql = format!("SELECT * FROM {}", layer);
    let statement = connection.prepare(&sql)?;

    let columns = statement.columns();

    let fields: Vec<Field> = columns
        .into_iter()
        .map(|column| Field::new(column.name(), get_data_type(column.decl_type()), false))
        .collect();

    Ok(Schema::new(fields))
}

fn get_columns(connection: &Connection, schema: &Schema, layer: &str) -> Vec<Arc<(dyn arrow::array::Array)>> {
    let names_and_types = schema
        .fields()
        .iter()
        .map(|field| (field.name(), field.data_type()));

    names_and_types
        .map(|(field_name, field_type)| {
            let mut statement = connection
                .prepare("SELECT :field_name FROM :layer")
                .expect("Failed to prepare statment.");
            let named_parameters = named_params! {
                ":field_name": field_name,
                ":layer": layer,
            };
            let rows = statement.query(named_parameters).expect("Failed to execute query.");
            let array_ref = match field_type {
                DataType::Boolean => {
                    let values: rusqlite::Result<Vec<Option<bool>>> = rows
                        .map(|row| {
                            let value = row.get(0).ok();
                            Ok(value)
                        })
                        .collect();
                    let data = BooleanArray::from_iter(values.unwrap());
                    Arc::new(data) as ArrayRef
                }
                DataType::Int8 => {
                    let values: rusqlite::Result<Vec<Option<i8>>> = rows
                        .map(|row| {
                            let value = row.get(0).ok();
                            Ok(value)
                        })
                        .collect();
                    let data: PrimitiveArray<Int8Type> = PrimitiveArray::from_iter(values.unwrap());
                    Arc::new(data) as ArrayRef
                }
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


fn list_layers(connection: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare("SELECT table_name FROM gpkg_contents")?;

    let rows = statement.query([])?;

    let values: rusqlite::Result<Vec<String>> = rows
        .map(|row| {
            let value: rusqlite::Result<String> = row.get(0);
            value
        })
        .collect();

    values
}

fn get_bounds(connection: &Connection, layer: &str) -> rusqlite::Result<[f64; 4]> {
    let mut statement = connection
        .prepare("SELECT min_x, min_y, max_x, max_y FROM gpkg_contents WHERE table_name = :layer")
        .expect("Failed to prepare statment.");
    let named_parameters = named_params! {
        ":layer": layer,
    };
    let row = statement.query_row(named_parameters, |row| {
        let values: [f64; 4] = [0, 1, 2, 3].map(|index: usize| {
            row.get_unwrap(index)
        });
        Ok(values)
    });

    row
} 

fn main() -> rusqlite::Result<()> {
    let path = "Data/bdline_gb.gpkg";
    let connection = Connection::open(path)?;

    let _layers = list_layers(&connection)?;

    let bounds = get_bounds(&connection, "english_region")?;

    // let schema = get_schema(&connection, "english_region")?;

    // let columns = get_columns(&connection, &schema, "english_region");

    // let _batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();

    dbg!(bounds);

    Ok(())
}
