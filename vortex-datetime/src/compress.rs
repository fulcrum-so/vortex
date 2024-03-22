use vortex::array::composite::CompositeEncoding;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::cast::cast;
use vortex::compute::flatten::flatten_primitive;
use vortex::datetime::{LocalDateTime, LocalDateTimeArray, LocalDateTimeExtension, TimeUnit};
use vortex::error::VortexResult;
use vortex::ptype::PType;

use crate::{DateTimeArray, DateTimeEncoding};

impl EncodingCompression for DateTimeEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        if array.encoding().id() != &CompositeEncoding::ID {
            return None;
        }

        let composite = array.as_composite();
        if !matches!(composite.id(), LocalDateTimeExtension::ID) {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let array = array.as_composite();
        match array.id() {
            LocalDateTimeExtension::ID => compress_localdatetime(
                array.as_typed::<LocalDateTime>(),
                like.map(|l| l.as_any().downcast_ref::<DateTimeArray>().unwrap()),
                ctx,
            ),
            _ => panic!("Unsupported composite ID {}", array.id()),
        }
    }
}

fn compress_localdatetime(
    array: LocalDateTimeArray,
    like: Option<&DateTimeArray>,
    ctx: CompressCtx,
) -> VortexResult<ArrayRef> {
    let underlying = flatten_primitive(cast(array.underlying(), &PType::I64.into())?.as_ref())?;

    let divisor = match array.metadata().time_unit() {
        TimeUnit::Ns => 1_000_000_000,
        TimeUnit::Us => 1_000_000,
        TimeUnit::Ms => 1_000,
        TimeUnit::S => 1,
    };

    let mut days = Vec::with_capacity(underlying.len());
    let mut seconds = Vec::with_capacity(underlying.len());
    let mut subsecond = Vec::with_capacity(underlying.len());

    for &t in underlying.typed_data::<i64>().iter() {
        days.push(t / (86_400 * divisor));
        seconds.push((t % (86_400 * divisor)) / divisor);
        subsecond.push((t % (86_400 * divisor)) % divisor);
    }

    Ok(DateTimeArray::new(
        ctx.named("days")
            .compress(&PrimitiveArray::from(days), like.map(|l| l.days()))?,
        ctx.named("seconds")
            .compress(&PrimitiveArray::from(seconds), like.map(|l| l.seconds()))?,
        ctx.named("subsecond").compress(
            &PrimitiveArray::from(subsecond),
            like.map(|l| l.subsecond()),
        )?,
        underlying.validity().cloned(),
        LocalDateTimeExtension::dtype(underlying.validity().is_some().into()),
    )
    .into_array())
}
