use crate::gpkg;

pub enum Dataset {
    Gpkg(rusqlite::Connection),
}

impl Dataset {
    pub fn open(path: &str) -> anyhow::Result<Dataset> {
        match path {
            path if path.ends_with(".gpkg") => {
                let connection = rusqlite::Connection::open(path)
                    .unwrap_or_else(|_| panic!("Failed to open {}", path));
                let dataset = Dataset::Gpkg(connection);
                Ok(dataset)
            }
            _ => unimplemented!(),
        }
    }
    pub fn list_layers(self) -> Vec<String> {
        match self {
            Dataset::Gpkg(connection) => gpkg::list_layers(&connection).unwrap(),
        }
    }
}
