use vortex::array::datetime::{LocalDateTimeArray, TimeUnit};
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::unary::cast::try_cast;
use vortex::{Array, ArrayTrait, IntoArray, IntoCanonical};
use vortex_dtype::PType;
use vortex_error::VortexResult;

pub fn compress_localdatetime(array: LocalDateTimeArray) -> VortexResult<(Array, Array, Array)> {
    let timestamps = try_cast(&array.timestamps(), PType::I64.into())?
        .into_canonical()?
        .into_primitive()?;

    let divisor = match array.time_unit() {
        TimeUnit::Ns => 1_000_000_000,
        TimeUnit::Us => 1_000_000,
        TimeUnit::Ms => 1_000,
        TimeUnit::S => 1,
    };

    let length = timestamps.len();
    let mut days = Vec::with_capacity(length);
    let mut seconds = Vec::with_capacity(length);
    let mut subsecond = Vec::with_capacity(length);

    for &t in timestamps.maybe_null_slice::<i64>().iter() {
        days.push(t / (86_400 * divisor));
        seconds.push((t % (86_400 * divisor)) / divisor);
        subsecond.push((t % (86_400 * divisor)) % divisor);
    }

    Ok((
        PrimitiveArray::from_vec(days, timestamps.validity()).into_array(),
        PrimitiveArray::from(seconds).into_array(),
        PrimitiveArray::from(subsecond).into_array(),
    ))
}
