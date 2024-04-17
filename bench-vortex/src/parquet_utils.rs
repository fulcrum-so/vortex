use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use vortex_error::VortexResult;

use crate::data_downloads::FileType;
use crate::CompressionRunStats;

pub fn sum_column_chunk_sizes(path: &Path) -> VortexResult<CompressionRunStats> {
    let file = File::open(path)?;
    let total_compressed_size = file.metadata()?.size();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();

    let mut compressed_sizes: HashMap<u64, u64> = HashMap::new();

    for i in 0..metadata.num_row_groups() {
        let row_group_metadata = metadata.row_group(i);

        // For each row group, iterate over its columns
        for j in 0..row_group_metadata.num_columns() {
            let column_chunk_metadata = row_group_metadata.column(j);
            // Add the sizes to the corresponding entries in the hash maps
            *compressed_sizes.entry(j as u64 + 1u64).or_insert(0) +=
                column_chunk_metadata.compressed_size() as u64;
        }
    }

    let stats = CompressionRunStats {
        schema: None,
        file_type: FileType::Parquet,
        uncompressed_size: None,
        total_compressed_size: Some(total_compressed_size),
        compressed_sizes,
        file_name: path
            .with_extension("")
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string(),
    };
    Ok(stats)
}
