use crate::wkb::WkbGeometry;
use arrow::{
    self,
    array::{
        ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, StringArray,
    },
    datatypes::{DataType, Field, Int32Type, Schema},
};
use binread::{BinRead, BinReaderExt};
use fallible_iterator::FallibleIterator;
use modular_bitfield::prelude::*;
use rusqlite::{self, named_params, Connection};
use std::{fmt::Error, iter::Iterator, sync::Arc};

fn get_data_type(sql_name: Option<&str>) -> DataType {
    sql_name
        .map(|name| match name {
            "BOOLEAN" => DataType::Boolean,
            "TINYINT" => DataType::Int8,
            "SMALLINT" => DataType::Int16,
            "MEDIUMINT" => DataType::Int32,
            "INT" | "INTEGER" => DataType::Int64,
            "FLOAT" | "DOUBLE" => DataType::Float32,
            "REAL" => DataType::Float64,
            name if name.starts_with("TEXT") => DataType::Utf8,
            name if name.starts_with("BLOB") => DataType::Utf8,
            "POINT" => {
                DataType::FixedSizeList(Box::new(Field::new("Point", DataType::Float64, true)), 2)
            }
            "DATE" | "DATETIME" => DataType::Utf8,
            "GEOMETRY" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
            | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => DataType::Binary,
            &_ => unimplemented!(),
        })
        .unwrap()
}

pub fn get_schema(connection: &Connection, layer: &str) -> rusqlite::Result<Schema> {
    let sql = format!("SELECT * FROM {}", layer);
    let statement = connection.prepare(&sql)?;

    let columns = statement.columns();

    let fields: Vec<Field> = columns
        .into_iter()
        .map(|column| Field::new(column.name(), get_data_type(column.decl_type()), true))
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

fn get_fields(
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
            match field_type {
                DataType::Boolean => generate_match_arm!(rows, bool, BooleanArray),
                DataType::Int8 => generate_match_arm!(rows, i8, Int8Array),
                DataType::Int16 => generate_match_arm!(rows, i16, Int16Array),
                DataType::Int32 => generate_match_arm!(rows, i32, Int32Array),
                DataType::Int64 => generate_match_arm!(rows, i64, Int64Array),
                DataType::Float32 => generate_match_arm!(rows, f32, Float32Array),
                DataType::Float64 => generate_match_arm!(rows, f64, Float64Array),
                DataType::Utf8 => generate_match_arm!(rows, String, StringArray),
                // DataType::Binary => {
                //     let values: rusqlite::Result<Vec<_>> = rows
                //         .map(|row| {
                //             let value: Vec<u8> = row.get_unwrap(0);
                //             let binary_array = BinaryArray::from_vec(vec![&value[..]]);
                //             Ok(binary_array.into_data())
                //         })
                //         .collect();
                //     let data = StructArray::from(values.unwrap());
                //     Arc::new(data) as ArrayRef
                // }
                _ => unimplemented!(),
            }
        })
        .collect()
}

// fn get_geometry(
//     connection: &Connection,
//     layer: &str,
//     schema: &Schema,
// ) -> Arc<(dyn arrow::array::Array)> {
//     let mut geometry_blob = connection
//         .blob_open(rusqlite::DatabaseName::Main, layer, "geometry", 1, true)
//         .unwrap();

//     let gpb: StandardGeoPackageBinary = geometry_blob.read_ne().unwrap();

//     let geometry_datatype = schema
//         .field_with_name("geometry")
//         .map(|field| field.data_type())
//         .unwrap();

//     match geometry_datatype {
//         DataType::FixedSizeList(Box::new(Field::new("Point", DataType::Float64, true)), 2) => {
//             let data: Result<[f64; 2], Error> = gpb.try_into();

//         }
//     }
// }

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

#[bitfield]
#[derive(BinRead, Debug)]
#[br(map = Self::from_bytes)]
pub struct Flags {
    byte_order: B1,
    envelope_size: B3,
    empty_geometry_flag: B1,
    gpb_type: B1,
    reserved: B2,
}

#[derive(BinRead, Debug)]
#[br(magic = b"GP")] // byte[2] magic = 0x4750;
#[br(little)]
pub struct GeoPackageBinaryHeader {
    version: u8,
    flags: Flags,
    // TODO use `flags.byte_order` to set endiness of `srs_id`
    srs_id: u32,
    // TODO use `flags.envelope_size` to set size of `envelope`
    envelope: [f64; 4],
}

#[derive(BinRead, Debug)]
pub struct StandardGeoPackageBinary {
    pub header: GeoPackageBinaryHeader,
    pub geometry: WkbGeometry,
}
