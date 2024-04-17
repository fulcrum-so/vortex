create table parquet as select * from comparison_results.csv
                        where file_type =='parquet';
create table vortex as select * from comparison_results.csv
                       where file_type =='vortex';
create table comparison as select vortex.dataset_name,
                                  vortex.file,
                                  vortex.column as column_name,
                                  vortex.column_type,
                                  vortex.column_compressed_size as vortex_column_size,
                                  parquet.column_compressed_size as parquet_column_compressed_size,
                                  vortex.column_compressed_size - parquet.column_compressed_size as abs_diff_column_size,
                                  vortex."column_compressed_size" / parquet."column_compressed_size" as relative_compression,
                                  vortex."column_compressed_size"/vortex."total_compressed_size" as ratio_total_compressed_size,
                                  vortex."total_compressed_size"/vortex."uncompressed_size" as vortex_column_compression_ratio,
                                  vortex.uncompressed_size as total_uncompressed_size,
                                  vortex.total_compressed_size as vortex_total_size,
                                  parquet.total_compressed_size as parquet_total_size,
                                  vortex.total_compressed_size/parquet.total_compressed_size as overall_rel_compress_ratio

                           from
                               vortex join parquet
                                           on vortex.file == parquet.file
                                           and vortex.column == parquet.column;


select * from comparison where relative_compression < 1.0 order by column_name;