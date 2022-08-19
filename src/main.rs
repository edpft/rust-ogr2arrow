use arrow::{
    self,
    array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, TimestampSecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use chrono::{DateTime, Utc};
use fallible_iterator::FallibleIterator;
use rusqlite::{self, named_params, Connection};
use std::{iter::Iterator, sync::Arc};

fn get_data_type(sql_name: Option<&str>) -> DataType {
    sql_name
        .map(|name| match name {
            "BOOLEAN" => DataType::Boolean,
            "TINYINT" => DataType::Int8,
            "SMALLINT" => DataType::Int16,
            "MEDIUMINT" => DataType::Int32,
            "INT" => DataType::Int64,
            "INTEGER" => DataType::Int64,
            "FLOAT" => DataType::Float32,
            "DOUBLE" => DataType::Float64,
            "REAL" => DataType::Float64,
            "TEXT" => DataType::Utf8,
            "BLOB" => DataType::Binary,
            "DATE" => DataType::Date32,
            "DATETIME" => DataType::Timestamp(TimeUnit::Second, None),
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

macro_rules! generate_match_arm {
    ($rows:ident, $rust_type:ty, $array_type:ty) => {{
        let values: rusqlite::Result<Vec<Option<$rust_type>>> = $rows
            .map(|row| {
                let value = row.get(0).ok();
                Ok(value)
            })
            .collect();
        let data = <$array_type>::from_iter(values.unwrap());
        Arc::new(data) as ArrayRef
    }};
}

fn get_columns(
    connection: &Connection,
    schema: &Schema,
    layer: &str,
) -> Vec<Arc<(dyn arrow::array::Array)>> {
    let names_and_types = schema
        .fields()
        .iter()
        .map(|field| (field.name(), field.data_type()));

    names_and_types
        .map(|(field_name, field_type)| {
            let sql = format!("SELECT {} FROM {}", field_name, layer);
            let mut statement = connection
                .prepare(&sql)
                .expect("Failed to prepare statment.");
            let rows = statement.query([]).expect("Failed to execute query.");
            // let mut statement = connection
            //     .prepare("SELECT :field_name FROM :layer")
            //     .expect("Failed to prepare statment.");
            // let rows = statement.query(named_params! {
            //     ":field_name": field_name,
            //     ":layer": layer,
            // }).expect("Failed to execute query.");
            let array_ref = match field_type {
                DataType::Boolean => generate_match_arm!(rows, bool, BooleanArray),
                DataType::Int8 => generate_match_arm!(rows, i8, Int8Array),
                DataType::Int16 => generate_match_arm!(rows, i16, Int16Array),
                DataType::Int32 => generate_match_arm!(rows, i32, Int32Array),
                DataType::Int64 => generate_match_arm!(rows, i64, Int64Array),
                DataType::Float32 => generate_match_arm!(rows, f32, Float32Array),
                DataType::Float64 => generate_match_arm!(rows, f64, Float64Array),
                DataType::Utf8 => generate_match_arm!(rows, String, StringArray),
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
    statement.query_row(named_parameters, |row| {
        let values: [f64; 4] = [0, 1, 2, 3].map(|index: usize| row.get_unwrap(index));
        Ok(values)
    })
}

fn main() -> rusqlite::Result<()> {
    let path = "Data/bdline_gb.gpkg";
    let connection = Connection::open(path)?;

    // let _layers = list_layers(&connection)?;

    // let _bounds = get_bounds(&connection, "english_region")?;

    let schema = get_schema(&connection, "gpkg_contents")?;

    let columns = get_columns(&connection, &schema, "gpkg_contents");

    let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();

    dbg!(batch);

    Ok(())
}
