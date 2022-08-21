use binread::BinRead;
use std::convert::TryFrom;
use std::convert::TryInto;

#[derive(BinRead, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[br(repr = u8)]
pub enum WkbByteOrder {
    Xdr = 0,
    Ndr = 1,
}

#[derive(BinRead, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[br(repr = u32)]
pub enum WkbGeometryType {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    Triangle = 17,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7,
    PolyhedralSurface = 15,
    Tin = 16,
}

#[derive(Debug, PartialEq, BinRead)]
#[br(little)]
pub struct Coordinate {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, PartialEq, BinRead)]
#[br(little)]
pub struct LinearRing {
    num_coordinates: u32,
    #[br(count = num_coordinates)]
    coordinates: Vec<Coordinate>,
}

macro_rules! derive_wkb_struct {
    ($name:ident, $geometry_type:ident, $count_field_name:ident, $field_name:ident, $child_geometry_type:ty) => {
        #[derive(Debug, PartialEq, BinRead)]
        #[br(little)]
        pub struct $name {
            pub byte_order: WkbByteOrder,
            #[br(is_big = (byte_order == WkbByteOrder::Xdr))]
            #[br(assert(wkb_type == WkbGeometryType::$geometry_type))]
            pub wkb_type: WkbGeometryType,
            #[br(is_big = (byte_order == WkbByteOrder::Xdr))]
            pub $count_field_name: u32,
            #[br(is_big = (byte_order == WkbByteOrder::Xdr))]
            #[br(count = $count_field_name)]
            pub $field_name: Vec<$child_geometry_type>,
        }
    };
    ($name:ident, $geometry_type:ident, $field_name:ident, $field_geometry_type:ty) => {
        #[derive(Debug, PartialEq, BinRead)]
        #[br(little)]
        pub struct $name {
            pub byte_order: WkbByteOrder,
            #[br(is_big = (byte_order == WkbByteOrder::Xdr))]
            #[br(assert(wkb_type == WkbGeometryType::$geometry_type))]
            pub wkb_type: WkbGeometryType,
            #[br(is_big = (byte_order == WkbByteOrder::Xdr))]
            pub $field_name: $field_geometry_type,
        }
    };
}

derive_wkb_struct!(WkbPoint, Point, point, Coordinate);
derive_wkb_struct!(WkbLineString, LineString, num_points, points, Coordinate);
derive_wkb_struct!(WkbPolygon, Polygon, num_rings, rings, LinearRing);
derive_wkb_struct!(WkbTriangle, Triangle, num_rings, rings, LinearRing);
derive_wkb_struct!(
    WkbPolyhedralSurface,
    PolyhedralSurface,
    num_polygons,
    polygons,
    WkbPolygon
);
derive_wkb_struct!(WkbTin, Tin, num_polygons, polygons, WkbPolygon);
derive_wkb_struct!(WkbMultiPoint, MultiPoint, num_points, points, WkbPoint);
derive_wkb_struct!(
    WkbMultiLineString,
    MultiLineString,
    num_line_strings,
    line_strings,
    WkbLineString
);
derive_wkb_struct!(
    WkbMultiPolygon,
    MultiPolygon,
    num_polygons,
    polygons,
    WkbPolygon
);
derive_wkb_struct!(
    WkbGeometryCollection,
    GeometryCollection,
    num_geometries,
    geometries,
    WkbGeometry
);

#[derive(BinRead, Debug, PartialEq)]
#[br(little)]
pub enum WkbGeometry {
    Point(WkbPoint),
    LineString(WkbLineString),
    Polygon(WkbPolygon),
    Triangle(WkbTriangle),
    MultiPoint(WkbMultiPoint),
    MultiLineString(WkbMultiLineString),
    MultiPolygon(WkbMultiPolygon),
    GeometryCollection(WkbGeometryCollection),
    PolyhedralSurface(WkbPolyhedralSurface),
    Tin(WkbTin),
}

impl TryInto<[f64; 2]> for Coordinate {
    type Error = ();

    fn try_into(self) -> Result<[f64; 2], Self::Error> {
        Ok([self.x, self.y])
    }
}

impl TryInto<[f64; 2]> for WkbPoint {
    type Error = ();

    fn try_into(self) -> Result<[f64; 2], Self::Error> {
        let coordinate: [f64; 2] = self.point.try_into()?;
        Ok(coordinate)
    }
}

// impl TryInto<FixedSizeListArray> for Coordinate {
//     type Error = ();

//     fn try_into(self) -> Result<FixedSizeListArray, Self::Error> {
//         let data_type =
//             DataType::FixedSizeList(Box::new(Field::new("Point", DataType::Float64, true)), 2);
//         let array_data = ArrayData::builder(data_type)
//             .len(2)
//             .add_buffer(Buffer::from_slice_ref(&[self.x, self.y]))
//             .build()
//             .unwrap();
//         Ok(FixedSizeListArray::from(array_data))
//     }
// }

// impl TryInto<FixedSizeListArray> for WkbPoint {
//     type Error = ();

//     fn try_into(self) -> Result<FixedSizeListArray, Self::Error> {
//         let data_type =
//             DataType::FixedSizeList(Box::new(Field::new("Point", DataType::Float64, true)), 2);
//         let array_data = ArrayData::builder(data_type)
//             .len(2)
//             .add_buffer(Buffer::from_slice_ref(&[self.point.x, self.point.y]))
//             .build()
//             .unwrap();
//         Ok(FixedSizeListArray::from(array_data))
//     }
// }

// impl TryInto<GenericListArray<FixedSizeListArray>> for LinearRing {
//     type Error = ();

//     fn try_into(self) -> Result<GenericListArray<FixedSizeListArray>, Self::Error> {
//         let field = Field::new(
//             "Polygon",
//             DataType::FixedSizeList(Box::new(Field::new("Point", DataType::Float64, true)), 2),
//             true,
//         );
//         let vector: Vec<(Field, ArrayRef)> = self
//             .coordinates
//             .into_iter()
//             .map(|coordinate| {
//                 let fixed_array: FixedSizeListArray = coordinate.try_into().unwrap();
//                 (field, fixed_array as ArrayRef)
//             })
//             .collect();

//         let list = StructArray::from(vector);
//     }
// }

impl TryInto<Vec<[f64; 2]>> for LinearRing {
    type Error = ();

    fn try_into(self) -> Result<Vec<[f64; 2]>, Self::Error> {
        let vector: Vec<[f64; 2]> = self
            .coordinates
            .into_iter()
            .map(|coordinate| {
                let array: [f64; 2] = coordinate.try_into().unwrap();
                array
            })
            .collect();
        Ok(vector)
    }
}

impl TryInto<Vec<Vec<[f64; 2]>>> for WkbPolygon {
    type Error = ();

    fn try_into(self) -> Result<Vec<Vec<[f64; 2]>>, Self::Error> {
        let vector: Vec<Vec<[f64; 2]>> = self
            .rings
            .into_iter()
            .map(|ring| {
                let vector: Vec<[f64; 2]> = ring.try_into().unwrap();
                vector
            })
            .collect();
        Ok(vector)
    }
}

impl TryInto<Vec<Vec<Vec<[f64; 2]>>>> for WkbMultiPolygon {
    type Error = ();

    fn try_into(self) -> Result<Vec<Vec<Vec<[f64; 2]>>>, Self::Error> {
        let vector: Vec<Vec<Vec<[f64; 2]>>> = self
            .polygons
            .into_iter()
            .map(|polygon| {
                let vector: Vec<Vec<[f64; 2]>> = polygon.try_into().unwrap();
                vector
            })
            .collect();
        Ok(vector)
    }
}

impl TryFrom<[f64; 2]> for WkbPoint {
    type Error = ();

    fn try_from(value: [f64; 2]) -> Result<Self, Self::Error> {
        Ok(WkbPoint {
            byte_order: WkbByteOrder::Ndr,
            wkb_type: WkbGeometryType::Point,
            point: Coordinate {
                x: value[0],
                y: value[1],
            },
        })
    }
}

impl TryFrom<Vec<Vec<Vec<[f64; 2]>>>> for WkbMultiPolygon {
    type Error = ();

    fn try_from(value: Vec<Vec<Vec<[f64; 2]>>>) -> Result<Self, Self::Error> {
        Ok(WkbMultiPolygon {
            byte_order: WkbByteOrder::Ndr,
            wkb_type: WkbGeometryType::MultiPolygon,
            num_polygons: value.len() as u32,
            polygons: value
                .iter()
                .map(|polygon| WkbPolygon {
                    byte_order: WkbByteOrder::Ndr,
                    wkb_type: WkbGeometryType::Polygon,
                    num_rings: polygon.len() as u32,
                    rings: polygon
                        .iter()
                        .map(|ring| LinearRing {
                            num_coordinates: ring.len() as u32,
                            coordinates: ring
                                .iter()
                                .map(|coordinate| Coordinate {
                                    x: coordinate[0],
                                    y: coordinate[1],
                                })
                                .collect(),
                        })
                        .collect(),
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod test {
    use binread::{io::Cursor, BinReaderExt};

    use super::*;

    #[test]
    fn read_wkb_point() {
        let expected_geometry = WkbGeometry::Point(WkbPoint {
            byte_order: WkbByteOrder::Ndr,
            wkb_type: WkbGeometryType::Point,
            point: Coordinate {
                x: 0.0f64,
                y: 0.0f64,
            },
        });

        let mut reader = Cursor::new(
            b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        );

        let recieved_geometry: WkbGeometry = reader.read_ne().unwrap();

        assert_eq!(expected_geometry, recieved_geometry);
    }

    #[test]
    fn read_wkb_linestring() {
        let expected_geometry = WkbGeometry::LineString(WkbLineString {
            byte_order: WkbByteOrder::Ndr,
            wkb_type: WkbGeometryType::LineString,
            num_points: 2u32,
            points: vec![
                Coordinate {
                    x: 0.0f64,
                    y: 0.0f64,
                },
                Coordinate {
                    x: 1.0f64,
                    y: 1.0f64,
                },
            ],
        });

        let mut reader = Cursor::new(
            b"\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?",
        );

        let recieved_geometry: WkbGeometry = reader.read_ne().unwrap();

        assert_eq!(expected_geometry, recieved_geometry);
    }
}
