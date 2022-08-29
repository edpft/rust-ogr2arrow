use std::{fmt::Display, path::Path};

use anyhow::Context;

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
}
