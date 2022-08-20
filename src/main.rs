use arrow::array::FixedSizeListArray;
use binread::{io::Cursor, BinReaderExt};
use ogr2arrow::{
    gpkg::{self, GeoPackageBinaryHeader, StandardGeoPackageBinary},
    wkb::WkbGeometry::{self, Point},
};
use rusqlite::Connection;

fn main() -> rusqlite::Result<()> {
    // let path = "Data/bdline_gb.gpkg";
    // let connection = Connection::open(path)?;

    // let statement = connection.prepare("SELECT * FROM boundary_line_ceremonial_counties")?;

    // let columns = statement.columns();

    // let fields: Vec<(&str, Option<&str>)> = columns
    //     .iter()
    //     .map(|column| (column.name(), column.decl_type()))
    //     .collect();

    // let mut geometry_blob = connection.blob_open(
    //     rusqlite::DatabaseName::Main,
    //     "boundary_line_ceremonial_counties",
    //     "geometry",
    //     1,
    //     false,
    // )?;

    let mut reader = Cursor::new(
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
    );

    let gpb: WkbGeometry = reader.read_ne().unwrap();

    // let vectors = match gpb.geometry {
    //     Point(geometry) => {
    //         let point: FixedSizeListArray = geometry.try_into().unwrap();
    //         point
    //     }
    //     _ => unimplemented!(),
    // };

    dbg!(gpb);

    Ok(())
}
