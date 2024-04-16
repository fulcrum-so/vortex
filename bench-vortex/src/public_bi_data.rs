use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

use enum_iterator::Sequence;
use fs_extra::dir::get_size;
use humansize::{format_size, DECIMAL};
use itertools::Itertools;
use log::info;
use reqwest::Url;
use vortex::OwnedArray;
use vortex_error::VortexError;

use crate::data_downloads::{
    decompress_bz2, download_data, parquet_to_lance, BenchmarkDataset, FileType,
};
use crate::public_bi_data::PBIDataset::*;
use crate::reader::{open_vortex, pbi_csv_format, write_csv_as_parquet, write_csv_to_vortex};
use crate::{idempotent, IdempotentPath};

lazy_static::lazy_static! {
    // NB: we do not expect this to change, otherwise we'd crawl the site and populate it at runtime
    // We will eventually switch over to self-hosting this data, at which time this map will need
    // to be updated once.
    static ref URLS: HashMap<PBIDataset, Vec<PBIUrl>> = HashMap::from([
            (AirlineSentiment, vec![
                PBIUrl::new("AirlineSentiment", "AirlineSentiment_1.csv.bz2")]),
            (Arade, vec!(PBIUrl::new("Arade","Arade_1.csv.bz2"))),
            (Bimbo, vec![
                PBIUrl::new("Bimbo", "Bimbo_1.csv.bz2")]),
            (CMSprovider, vec![
                PBIUrl::new("CMSprovider", "CMSprovider_1.csv.bz2"),
                PBIUrl::new("CMSprovider", "CMSprovider_2.csv.bz2")]),
            (CityMaxCapita, vec![
                PBIUrl::new("CityMaxCapita", "CityMaxCapita_1.csv.bz2")]),
            (CommonGovernment, vec![
                PBIUrl::new("CommonGovernment", "CommonGovernment_1.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_2.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_3.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_4.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_5.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_6.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_7.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_8.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_9.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_10.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_11.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_12.csv.bz2"),
                PBIUrl::new("CommonGovernment", "CommonGovernment_13.csv.bz2")]),
            (Corporations, vec![
                PBIUrl::new("Corporations", "Corporations_1.csv.bz2")]),
            (Eixo, vec![
                PBIUrl::new("Eixo", "Eixo_1.csv.bz2")]),
            (Euro2016, vec![
                PBIUrl::new("Euro2016", "Euro2016_1.csv.bz2")]),
            (Food, vec![
                PBIUrl::new("Food", "Food_1.csv.bz2")]),
            (Generico, vec![
                PBIUrl::new("Generico", "Generico_1.csv.bz2"),
                PBIUrl::new("Generico", "Generico_2.csv.bz2"),
                PBIUrl::new("Generico", "Generico_3.csv.bz2"),
                PBIUrl::new("Generico", "Generico_4.csv.bz2"),
                PBIUrl::new("Generico", "Generico_5.csv.bz2"),]),
            (HashTags, vec![
                PBIUrl::new("HashTags", "HashTags_1.csv.bz2")]),
            (Hatred, vec![
                PBIUrl::new("Hatred", "Hatred_1.csv.bz2")]),
            (IGlocations1, vec![
                PBIUrl::new("IGlocations1", "IGlocations1_1.csv.bz2")]),
            (IGlocations2, vec![
                PBIUrl::new("IGlocations2", "IGlocations2_1.csv.bz2"),
                PBIUrl::new("IGlocations2", "IGlocations2_2.csv.bz2")]),
            (IUBLibrary, vec![
                PBIUrl::new("IUBLibrary", "IUBLibrary_1.csv.bz2")]),
            (MLB, vec![
                PBIUrl::new("MLB", "MLB_1.csv.bz2"),
                PBIUrl::new("MLB", "MLB_2.csv.bz2"),
                PBIUrl::new("MLB", "MLB_3.csv.bz2"),
                PBIUrl::new("MLB", "MLB_4.csv.bz2"),
                PBIUrl::new("MLB", "MLB_5.csv.bz2"),
                PBIUrl::new("MLB", "MLB_6.csv.bz2"),
                PBIUrl::new("MLB", "MLB_7.csv.bz2"),
                PBIUrl::new("MLB", "MLB_8.csv.bz2"),
                PBIUrl::new("MLB", "MLB_9.csv.bz2"),
                PBIUrl::new("MLB", "MLB_10.csv.bz2"),
                PBIUrl::new("MLB", "MLB_11.csv.bz2"),
                PBIUrl::new("MLB", "MLB_12.csv.bz2"),
                PBIUrl::new("MLB", "MLB_13.csv.bz2"),
                PBIUrl::new("MLB", "MLB_14.csv.bz2"),
                PBIUrl::new("MLB", "MLB_15.csv.bz2"),
                PBIUrl::new("MLB", "MLB_16.csv.bz2"),
                PBIUrl::new("MLB", "MLB_17.csv.bz2"),
                PBIUrl::new("MLB", "MLB_18.csv.bz2"),
                PBIUrl::new("MLB", "MLB_19.csv.bz2"),
                PBIUrl::new("MLB", "MLB_20.csv.bz2"),
                PBIUrl::new("MLB", "MLB_21.csv.bz2"),
                PBIUrl::new("MLB", "MLB_22.csv.bz2"),
                PBIUrl::new("MLB", "MLB_23.csv.bz2"),
                PBIUrl::new("MLB", "MLB_24.csv.bz2"),
                PBIUrl::new("MLB", "MLB_25.csv.bz2"),
                PBIUrl::new("MLB", "MLB_26.csv.bz2"),
                PBIUrl::new("MLB", "MLB_27.csv.bz2"),
                PBIUrl::new("MLB", "MLB_28.csv.bz2"),
                PBIUrl::new("MLB", "MLB_29.csv.bz2"),
                PBIUrl::new("MLB", "MLB_30.csv.bz2"),
                PBIUrl::new("MLB", "MLB_31.csv.bz2"),
                PBIUrl::new("MLB", "MLB_32.csv.bz2"),
                PBIUrl::new("MLB", "MLB_33.csv.bz2"),
                PBIUrl::new("MLB", "MLB_34.csv.bz2"),
                PBIUrl::new("MLB", "MLB_35.csv.bz2"),
                PBIUrl::new("MLB", "MLB_36.csv.bz2"),
                PBIUrl::new("MLB", "MLB_37.csv.bz2"),
                PBIUrl::new("MLB", "MLB_38.csv.bz2"),
                PBIUrl::new("MLB", "MLB_39.csv.bz2"),
                PBIUrl::new("MLB", "MLB_40.csv.bz2"),
                PBIUrl::new("MLB", "MLB_41.csv.bz2"),
                PBIUrl::new("MLB", "MLB_42.csv.bz2"),
                PBIUrl::new("MLB", "MLB_43.csv.bz2"),
                PBIUrl::new("MLB", "MLB_44.csv.bz2"),
                PBIUrl::new("MLB", "MLB_45.csv.bz2"),
                PBIUrl::new("MLB", "MLB_46.csv.bz2"),
                PBIUrl::new("MLB", "MLB_47.csv.bz2"),
                PBIUrl::new("MLB", "MLB_48.csv.bz2"),
                PBIUrl::new("MLB", "MLB_49.csv.bz2"),
                PBIUrl::new("MLB", "MLB_50.csv.bz2"),
                PBIUrl::new("MLB", "MLB_51.csv.bz2"),
                PBIUrl::new("MLB", "MLB_52.csv.bz2"),
                PBIUrl::new("MLB", "MLB_53.csv.bz2"),
                PBIUrl::new("MLB", "MLB_54.csv.bz2"),
                PBIUrl::new("MLB", "MLB_55.csv.bz2"),
                PBIUrl::new("MLB", "MLB_56.csv.bz2"),
                PBIUrl::new("MLB", "MLB_57.csv.bz2"),
                PBIUrl::new("MLB", "MLB_58.csv.bz2"),
                PBIUrl::new("MLB", "MLB_59.csv.bz2"),
                PBIUrl::new("MLB", "MLB_60.csv.bz2"),
                PBIUrl::new("MLB", "MLB_61.csv.bz2"),
                PBIUrl::new("MLB", "MLB_62.csv.bz2"),
                PBIUrl::new("MLB", "MLB_63.csv.bz2"),
                PBIUrl::new("MLB", "MLB_64.csv.bz2"),
                PBIUrl::new("MLB", "MLB_65.csv.bz2"),
                PBIUrl::new("MLB", "MLB_66.csv.bz2"),
                PBIUrl::new("MLB", "MLB_67.csv.bz2"),
                PBIUrl::new("MLB", "MLB_68.csv.bz2")]),
            (MedPayment1, vec![
                PBIUrl::new("MedPayment1", "MedPayment1_1.csv.bz2")]),
            (MedPayment2, vec![
                PBIUrl::new("MedPayment2", "MedPayment2_1.csv.bz2")]),
            (Medicare1, vec![
                PBIUrl::new("Medicare1", "Medicare1_1.csv.bz2"),
                PBIUrl::new("Medicare1", "Medicare1_2.csv.bz2")]),
            (Medicare2, vec![
                PBIUrl::new("Medicare2", "Medicare2_1.csv.bz2"),
                PBIUrl::new("Medicare2", "Medicare2_2.csv.bz2")]),
            (Medicare3, vec![
                PBIUrl::new("Medicare3", "Medicare3_1.csv.bz2")]),
            (Motos, vec![
                PBIUrl::new("Motos", "Motos_1.csv.bz2"),
                PBIUrl::new("Motos", "Motos_2.csv.bz2")]),
            (MulheresMil, vec![
                PBIUrl::new("MulheresMil", "MulheresMil_1.csv.bz2")]),
            (NYC, vec![
                PBIUrl::new("NYC", "NYC_1.csv.bz2"),
                PBIUrl::new("NYC", "NYC_2.csv.bz2")]),
            (PanCreactomy1, vec![
                PBIUrl::new("PanCreactomy1", "PanCreactomy1_1.csv.bz2")]),
            (PanCreactomy2, vec![
                PBIUrl::new("PanCreactomy2", "PanCreactomy2_1.csv.bz2"),
                PBIUrl::new("PanCreactomy2", "PanCreactomy2_2.csv.bz2")]),
            (Physicians, vec![
                PBIUrl::new("Physicians", "Physicians_1.csv.bz2")]),
            (Provider, vec![
                PBIUrl::new("Provider", "Provider_1.csv.bz2"),
                PBIUrl::new("Provider", "Provider_2.csv.bz2"),
                PBIUrl::new("Provider", "Provider_3.csv.bz2"),
                PBIUrl::new("Provider", "Provider_4.csv.bz2"),
                PBIUrl::new("Provider", "Provider_5.csv.bz2"),
                PBIUrl::new("Provider", "Provider_6.csv.bz2"),
                PBIUrl::new("Provider", "Provider_7.csv.bz2"),
                PBIUrl::new("Provider", "Provider_8.csv.bz2")]),
            (RealEstate1, vec![
                PBIUrl::new("RealEstate1", "RealEstate1_1.csv.bz2"),
                PBIUrl::new("RealEstate1", "RealEstate1_2.csv.bz2")]),
            (RealEstate2, vec![
                PBIUrl::new("RealEstate2", "RealEstate2_1.csv.bz2"),
                PBIUrl::new("RealEstate2", "RealEstate2_2.csv.bz2"),
                PBIUrl::new("RealEstate2", "RealEstate2_3.csv.bz2"),
                PBIUrl::new("RealEstate2", "RealEstate2_4.csv.bz2"),
                PBIUrl::new("RealEstate2", "RealEstate2_5.csv.bz2"),
                PBIUrl::new("RealEstate2", "RealEstate2_6.csv.bz2"),
                PBIUrl::new("RealEstate2", "RealEstate2_7.csv.bz2")]),
            (Redfin1, vec![
                PBIUrl::new("Redfin1", "Redfin1_1.csv.bz2"),
                PBIUrl::new("Redfin1", "Redfin1_2.csv.bz2"),
                PBIUrl::new("Redfin1", "Redfin1_3.csv.bz2"),
                PBIUrl::new("Redfin1", "Redfin1_4.csv.bz2")]),
            (Redfin2, vec![
                PBIUrl::new("Redfin2", "Redfin2_1.csv.bz2"),
                PBIUrl::new("Redfin2", "Redfin2_2.csv.bz2"),
                PBIUrl::new("Redfin2", "Redfin2_3.csv.bz2")]),
            (Redfin3, vec![
                PBIUrl::new("Redfin3", "Redfin3_1.csv.bz2"),
                PBIUrl::new("Redfin3", "Redfin3_2.csv.bz2")]),
            (Redfin4, vec![
                PBIUrl::new("Redfin4", "Redfin4_1.csv.bz2")]),
            (Rentabilidad, vec![
                PBIUrl::new("Rentabilidad", "Rentabilidad_1.csv.bz2"),
                PBIUrl::new("Rentabilidad", "Rentabilidad_2.csv.bz2"),
                PBIUrl::new("Rentabilidad", "Rentabilidad_3.csv.bz2"),
                PBIUrl::new("Rentabilidad", "Rentabilidad_4.csv.bz2"),
                PBIUrl::new("Rentabilidad", "Rentabilidad_5.csv.bz2"),
                PBIUrl::new("Rentabilidad", "Rentabilidad_6.csv.bz2"),
                PBIUrl::new("Rentabilidad", "Rentabilidad_7.csv.bz2"),
                PBIUrl::new("Rentabilidad", "Rentabilidad_8.csv.bz2"),
                PBIUrl::new("Rentabilidad", "Rentabilidad_9.csv.bz2")]),
            (Romance, vec![
                PBIUrl::new("Romance", "Romance_1.csv.bz2"),
                PBIUrl::new("Romance", "Romance_2.csv.bz2")]),
            (SalariesFrance, vec![
                PBIUrl::new("SalariesFrance", "SalariesFrance_1.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_2.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_3.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_4.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_5.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_6.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_7.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_8.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_9.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_10.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_11.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_12.csv.bz2"),
                PBIUrl::new("SalariesFrance", "SalariesFrance_13.csv.bz2")]),
            (TableroSistemaPenal, vec![
                PBIUrl::new("TableroSistemaPenal", "TableroSistemaPenal_1.csv.bz2"),
                PBIUrl::new("TableroSistemaPenal", "TableroSistemaPenal_2.csv.bz2"),
                PBIUrl::new("TableroSistemaPenal", "TableroSistemaPenal_3.csv.bz2"),
                PBIUrl::new("TableroSistemaPenal", "TableroSistemaPenal_4.csv.bz2"),
                PBIUrl::new("TableroSistemaPenal", "TableroSistemaPenal_5.csv.bz2"),
                PBIUrl::new("TableroSistemaPenal", "TableroSistemaPenal_6.csv.bz2"),
                PBIUrl::new("TableroSistemaPenal", "TableroSistemaPenal_7.csv.bz2"),
                PBIUrl::new("TableroSistemaPenal", "TableroSistemaPenal_8.csv.bz2")]),
            (Taxpayer, vec![
                PBIUrl::new("Taxpayer", "Taxpayer_1.csv.bz2"),
                PBIUrl::new("Taxpayer", "Taxpayer_2.csv.bz2"),
                PBIUrl::new("Taxpayer", "Taxpayer_3.csv.bz2"),
                PBIUrl::new("Taxpayer", "Taxpayer_4.csv.bz2"),
                PBIUrl::new("Taxpayer", "Taxpayer_5.csv.bz2"),
                PBIUrl::new("Taxpayer", "Taxpayer_6.csv.bz2"),
                PBIUrl::new("Taxpayer", "Taxpayer_7.csv.bz2"),
                PBIUrl::new("Taxpayer", "Taxpayer_8.csv.bz2"),
                PBIUrl::new("Taxpayer", "Taxpayer_9.csv.bz2"),
                PBIUrl::new("Taxpayer", "Taxpayer_10.csv.bz2")]),
            (Telco, vec![
                PBIUrl::new("Telco", "Telco_1.csv.bz2")]),
            (TrainsUK1, vec![
                PBIUrl::new("TrainsUK1", "TrainsUK1_1.csv.bz2"),
                PBIUrl::new("TrainsUK1", "TrainsUK1_2.csv.bz2"),
                PBIUrl::new("TrainsUK1", "TrainsUK1_3.csv.bz2"),
                PBIUrl::new("TrainsUK1", "TrainsUK1_4.csv.bz2")]),
            (TrainsUK2, vec![
                PBIUrl::new("TrainsUK2", "TrainsUK2_1.csv.bz2"),
                PBIUrl::new("TrainsUK2", "TrainsUK2_2.csv.bz2")]),
            (USCensus, vec![
                PBIUrl::new("USCensus", "USCensus_1.csv.bz2"),
                PBIUrl::new("USCensus", "USCensus_2.csv.bz2"),
                PBIUrl::new("USCensus", "USCensus_3.csv.bz2")]),
            (Uberlandia, vec![
                PBIUrl::new("Uberlandia", "Uberlandia_1.csv.bz2")]),
            (Wins, vec![
                PBIUrl::new("Wins", "Wins_1.csv.bz2"),
                PBIUrl::new("Wins", "Wins_2.csv.bz2"),
                PBIUrl::new("Wins", "Wins_3.csv.bz2"),
                PBIUrl::new("Wins", "Wins_4.csv.bz2")]),
            (YaleLanguages, vec![
                PBIUrl::new("YaleLanguages", "YaleLanguages_1.csv.bz2"),
                PBIUrl::new("YaleLanguages", "YaleLanguages_2.csv.bz2"),
                PBIUrl::new("YaleLanguages", "YaleLanguages_3.csv.bz2"),
                PBIUrl::new("YaleLanguages", "YaleLanguages_4.csv.bz2"),
                PBIUrl::new("YaleLanguages", "YaleLanguages_5.csv.bz2")]),
        ]);
}

impl PBIDataset {
    pub fn dataset_name(&self) -> &str {
        let url = URLS.get(self).unwrap();
        url.first().unwrap().dataset_name
    }

    fn list_files(&self, file_type: FileType) -> Vec<PathBuf> {
        let urls = URLS.get(self).unwrap();
        urls.iter()
            .map(|url| self.get_file_path(url, file_type))
            .collect_vec()
    }

    fn get_file_path(&self, url: &PBIUrl, file_type: FileType) -> PathBuf {
        let extension = match file_type {
            FileType::Csv => "csv",
            FileType::Parquet => "parquet",
            FileType::Vortex => "vortex",
            FileType::Lance => "lance",
        };

        "PBI"
            .to_data_path()
            .join(self.dataset_name())
            .join(extension)
            .join(url.file_name.strip_suffix(".csv.bz2").unwrap())
            .with_extension(extension)
    }

    fn download_bzip(&self) {
        let urls = URLS.get(self).unwrap();
        self.dataset_name();
        urls.iter().for_each(|url| {
            let fname = self.get_bzip_path(url);
            download_data(fname, url.to_url_string().as_str());
        });
    }

    fn get_bzip_path(&self, url: &PBIUrl) -> PathBuf {
        "PBI"
            .to_data_path()
            .join(self.dataset_name())
            .join("bzip2")
            .join(url.file_name)
    }

    fn unzip(&self) {
        for url in URLS.get(self).unwrap() {
            let bzipped = self.get_bzip_path(url);
            let unzipped_csv = self.get_file_path(url, FileType::Csv);
            decompress_bz2(bzipped, unzipped_csv);
        }
    }
}

#[derive(Debug)]
struct PBIUrl {
    dataset_name: &'static str,
    file_name: &'static str,
}

impl PBIUrl {
    fn new(dataset_name: &'static str, file_name: &'static str) -> Self {
        PBIUrl {
            dataset_name,
            file_name,
        }
    }
    fn to_url_string(&self) -> Url {
        Url::parse("https://homepages.cwi.nl/~boncz/PublicBIbenchmark/")
            .unwrap()
            .join(format!("{}/", self.dataset_name).as_str())
            .unwrap()
            .join(self.file_name)
            .unwrap()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Sequence)]
pub enum PBIDataset {
    AirlineSentiment,
    Arade,
    Bimbo,
    CMSprovider,
    CityMaxCapita,
    CommonGovernment,
    Corporations,
    Eixo,
    Euro2016,
    Food,
    Generico,
    HashTags,
    Hatred,
    IGlocations1,
    IGlocations2,
    IUBLibrary,
    MLB,
    MedPayment1,
    MedPayment2,
    Medicare1,
    Medicare2,
    Medicare3,
    Motos,
    MulheresMil,
    NYC,
    PanCreactomy1,
    PanCreactomy2,
    Physicians,
    Provider,
    RealEstate1,
    RealEstate2,
    Redfin1,
    Redfin2,
    Redfin3,
    Redfin4,
    Rentabilidad,
    Romance,
    SalariesFrance,
    TableroSistemaPenal,
    Taxpayer,
    Telco,
    TrainsUK1,
    TrainsUK2,
    USCensus,
    Uberlandia,
    Wins,
    YaleLanguages,
}

pub enum BenchmarkDatasets {
    PBI(PBIDataset),
}

impl BenchmarkDataset for BenchmarkDatasets {
    fn as_uncompressed(&self) {
        match self {
            BenchmarkDatasets::PBI(dataset) => {
                dataset.download_bzip();
                dataset.unzip();
            }
        }
    }

    fn write_as_parquet(&self) {
        for f in self.list_files(FileType::Csv) {
            let output_fname = f
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .strip_suffix(".csv")
                .unwrap();
            let compressed = idempotent(
                &path_for_file_type(self, output_fname, "parquet"),
                |output_path| {
                    let mut write = File::create(output_path).unwrap();
                    let csv_input = f;
                    write_csv_as_parquet(csv_input, pbi_csv_format(), &mut write)
                },
            )
            .expect("Failed to compress to parquet");
            let pq_size = compressed.metadata().unwrap().size();
            info!(
                "Parquet size: {}, {}B",
                format_size(pq_size, DECIMAL),
                pq_size
            );
        }
    }

    /// Compresses the CSV files to Vortex format. Does NOT write any data to disk.
    /// Used for benchmarking.
    fn compress_to_vortex(&self) -> Vec<OwnedArray> {
        vec![]
        // self.list_files(FileType::Csv)
        //     .into_iter()
        //     .map(|csv_input| {
        //         info!("Compressing {} to vortex", csv_input.to_str().unwrap());
        //         compress_csv_to_vortex(csv_input, pbi_csv_format()).1
        //     })
        //     .collect_vec()
    }

    fn write_as_vortex(&self) {
        for f in self.list_files(FileType::Csv) {
            info!("Compressing and writing {} to vortex", f.to_str().unwrap());
            let output_fname = f
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .strip_suffix(".csv")
                .unwrap();

            let compressed = idempotent(
                &path_for_file_type(self, output_fname, "vortex"),
                |output_path| {
                    let mut write = File::create(output_path).unwrap();
                    let csv_input = f;
                    write_csv_to_vortex(csv_input, pbi_csv_format(), &mut write)
                },
            )
            .expect("Failed to compress to vortex");
            let from_vortex = open_vortex(&compressed).unwrap();
            let vx_size = from_vortex.nbytes();

            info!(
                "Vortex size: {}, {}B",
                format_size(vx_size as u64, DECIMAL),
                vx_size
            );
            info!("{}\n\n", from_vortex.tree_display());
        }
    }

    fn write_as_lance(&self) {
        for f in self.list_files(FileType::Csv) {
            info!("Compressing {} to lance", f.to_str().unwrap());
            let output_fname = f
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .strip_suffix(".csv")
                .unwrap();
            let compressed = idempotent(
                &path_for_file_type(self, output_fname, "lance"),
                |output_path| {
                    Ok::<_, VortexError>(parquet_to_lance(
                        output_path,
                        path_for_file_type(self, output_fname, "parquet").as_path(),
                    ))
                },
            )
            .expect("Failed to compress to lance");

            let lance_dir_bytes_exact = get_size(compressed).unwrap();
            let lance_dir_size = humansize::format_size(lance_dir_bytes_exact, DECIMAL);

            info!(
                "Lance directory aggregate size: {}, {}B",
                lance_dir_size, lance_dir_bytes_exact
            );
        }
    }

    fn list_files(&self, types: FileType) -> Vec<PathBuf> {
        match self {
            BenchmarkDatasets::PBI(dataset) => dataset.list_files(types),
        }
    }

    fn directory_location(&self) -> PathBuf {
        match self {
            BenchmarkDatasets::PBI(dataset) => "PBI".to_data_path().join(dataset.dataset_name()),
        }
    }
}

fn path_for_file_type(
    dataset: &impl BenchmarkDataset,
    output_fname: &str,
    file_type: &str,
) -> PathBuf {
    dataset
        .directory_location()
        .join(file_type)
        .join(format!("{}.{}", output_fname, file_type))
}
