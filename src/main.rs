use ogr2arrow::dataset::Dataset;

fn main() -> anyhow::Result<()> {
    let dataset = Dataset::open("Data/point.gpkg".to_string())?;

    let layers = dataset.list_layers()?;

    dbg!(layers);

    Ok(())
}
