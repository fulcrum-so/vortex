use std::fs::File;

use bench_vortex::reader::open_vortex;
use bench_vortex::setup_logger;
use bench_vortex::taxi_data::taxi_data_vortex;
use log::LevelFilter;
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::take::take;
use vortex::serde::context::SerdeContext;
use vortex_error::VortexResult;
use vortex_ipc::iter::FallibleLendingIterator;
use vortex_ipc::reader::StreamReader;
use vortex_ipc::writer::StreamWriter;

pub fn main() -> VortexResult<()> {
    setup_logger(LevelFilter::Error);

    let array = open_vortex(&taxi_data_vortex())?;
    println!("Array {}", &array);

    //let ipc = idempotent("ipc.vortex", |path| {
    let ipc = "bench-vortex/data/ipc.vortex";
    let mut write = File::create("bench-vortex/data/ipc.vortex")?;
    let ctx = SerdeContext::default();
    let mut writer = StreamWriter::try_new(&mut write, ctx)?;
    writer.write(&array)?;
    //})?;

    // Now try to read from the IPC stream.
    let mut read = File::open(ipc)?;
    let mut ipc_reader = StreamReader::try_new(&mut read)?;

    // We know we only wrote a single array.
    // TODO(ngates): create an option to skip the multi-array reader?
    let mut array_reader = ipc_reader.next()?.unwrap();
    println!("DType: {:?}", array_reader.dtype());
    // Read some number of chunks from the stream.
    while let Some(chunk) = array_reader.next().unwrap() {
        println!("VIEW: {:?}", chunk);
        let taken = chunk
            .with_array(|a| take(a, &PrimitiveArray::from(vec![0, 1, 0, 1])))
            .unwrap();
        println!("Taken: {}", &taken);
    }

    Ok(())
}
