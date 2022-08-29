use ogr2arrow::dataset::Dataset;

fn main() -> anyhow::Result<()> {
    let dataset = Dataset::open("Data/point.gpkg")?;

    let layer = dataset.get_layer("point")?;

    dbg!(layer);

    Ok(())
}
