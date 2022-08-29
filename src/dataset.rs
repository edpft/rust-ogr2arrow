use std::{fmt::Display, path::Path};

use anyhow::Context;
use arrow::record_batch::RecordBatch;

use crate::gpkg;

pub enum Dataset {
    Gpkg(rusqlite::Connection),
}

impl Dataset {
    pub fn open<P: AsRef<Path> + Display>(path: P) -> anyhow::Result<Dataset> {
        let path = path.as_ref();
        match path.extension() {
            None => unimplemented!(),
            Some(extension) => match extension.to_str() {
                Some("gpkg") => {
                    let connection = rusqlite::Connection::open(&path)
                        .context(format!("Failed to open {}", &path.display()))?;
                    let dataset = Dataset::Gpkg(connection);
                    Ok(dataset)
                }
                _ => unimplemented!(),
            },
        }
    }
    pub fn list_layers(self) -> anyhow::Result<Vec<String>> {
        let layers = match self {
            Dataset::Gpkg(connection) => {
                gpkg::list_layers(&connection).context("Failed to list layers")?
            }
        };
        Ok(layers)
    }
    pub fn get_layer(self, layer_name: &str) -> anyhow::Result<RecordBatch> {
        let layer = match self {
            Dataset::Gpkg(connection) => gpkg::get_layer(&connection, layer_name)
                .context(format!("Failed to get {}", layer_name))?,
        };
        Ok(layer)
    }
}
