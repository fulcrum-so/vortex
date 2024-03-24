use std::path::PathBuf;

use crate::idempotent;

pub fn download_taxi_data() -> PathBuf {
    idempotent("yellow-tripdata-2023-11.parquet", |file| {
        reqwest::blocking::get(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet",
        )
        .unwrap()
        .copy_to(file)
        .unwrap();
    })
}
