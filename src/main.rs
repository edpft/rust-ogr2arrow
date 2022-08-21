use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use ogr2arrow::gpkg::{get_fields, get_schema};
use rusqlite::Connection;

fn main() -> rusqlite::Result<()> {
    let connection = Connection::open("Data/point.gpkg")?;
    let layer = "point";

    let schema = get_schema(&connection, layer)?;

    let fields = get_fields(&connection, &schema, layer);

    let batch = RecordBatch::try_new(Arc::new(schema), fields).unwrap();

    dbg!(batch);

    Ok(())
}
