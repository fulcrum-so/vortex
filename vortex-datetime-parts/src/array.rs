use serde::{Deserialize, Serialize};
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, ToArrayData};
use vortex_error::vortex_bail;

impl_encoding!("vortex.datetimeparts", DateTimeParts);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DateTimePartsMetadata {
    days_dtype: DType,
    seconds_dtype: DType,
    subseconds_dtype: DType,
    validity: ValidityMetadata,
}

impl DateTimePartsArray<'_> {
    pub fn try_new(
        dtype: DType,
        days: Array,
        seconds: Array,
        subsecond: Array,
        validity: Validity,
    ) -> VortexResult<Self> {
        if !days.dtype().is_int() {
            vortex_bail!(MismatchedTypes: "any integer", days.dtype());
        }
        if !seconds.dtype().is_int() {
            vortex_bail!(MismatchedTypes: "any integer", seconds.dtype());
        }
        if !subsecond.dtype().is_int() {
            vortex_bail!(MismatchedTypes: "any integer", subsecond.dtype());
        }

        let length = days.len();
        if length != seconds.len() || length != subsecond.len() {
            vortex_bail!(
                "Mismatched lengths {} {} {}",
                days.len(),
                seconds.len(),
                subsecond.len()
            );
        }

        let mut children = Vec::with_capacity(4);
        children.extend([
            days.to_array_data(),
            seconds.to_array_data(),
            subsecond.to_array_data(),
        ]);
        let validity_metadata = validity.to_metadata(length)?;
        if let Some(validity) = validity.into_array_data() {
            children.push(validity);
        }

        Self::try_from_parts(
            dtype,
            DateTimePartsMetadata {
                days_dtype: days.dtype().clone(),
                seconds_dtype: seconds.dtype().clone(),
                subseconds_dtype: subsecond.dtype().clone(),
                validity: validity_metadata,
            },
            children.into(),
            StatsSet::new(),
        )
    }

    pub fn days(&self) -> Array {
        self.array()
            .child(0, &self.metadata().days_dtype)
            .expect("Missing days array")
    }

    pub fn seconds(&self) -> Array {
        self.array()
            .child(1, &self.metadata().seconds_dtype)
            .expect("Missing seconds array")
    }

    pub fn subsecond(&self) -> Array {
        self.array()
            .child(2, &self.metadata().subseconds_dtype)
            .expect("Missing subsecond array")
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(3, &Validity::DTYPE))
    }
}

impl ArrayFlatten for DateTimePartsArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        todo!()
    }
}

impl ArrayValidity for DateTimePartsArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for DateTimePartsArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("days", &self.days())?;
        visitor.visit_child("seconds", &self.seconds())?;
        visitor.visit_child("subsecond", &self.subsecond())
    }
}

impl ArrayStatisticsCompute for DateTimePartsArray<'_> {}

impl ArrayTrait for DateTimePartsArray<'_> {
    fn len(&self) -> usize {
        self.days().len()
    }
}
