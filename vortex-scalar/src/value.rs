use std::any;

use vortex_dtype::Nullability;
use vortex_error::{vortex_bail, VortexResult};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct ScalarValue<T> {
    nullability: Nullability,
    value: Option<T>,
}

impl<T> ScalarValue<T> {
    pub fn try_new(value: Option<T>, nullability: Nullability) -> VortexResult<Self> {
        if value.is_none() && nullability == Nullability::NonNullable {
            vortex_bail!("Value cannot be None for NonNullable Scalar");
        }
        Ok(Self { value, nullability })
    }

    pub fn non_nullable(value: T) -> Self {
        Self::try_new(Some(value), Nullability::NonNullable).unwrap_or_else(|e| {
            unreachable!(
                "Failed to create non-nullable scalar of type {}: {}",
                any::type_name::<T>(),
                e
            )
        })
    }

    pub fn nullable(value: T) -> Self {
        Self::try_new(Some(value), Nullability::Nullable).unwrap_or_else(|e| {
            unreachable!(
                "Failed to create nullable scalar of type {}: {}",
                any::type_name::<T>(),
                e
            )
        })
    }

    pub fn some(value: T) -> Self {
        Self::try_new(Some(value), Nullability::default()).unwrap_or_else(|e| {
            unreachable!(
                "Failed to create \"Some\" scalar of type {}: {}",
                any::type_name::<T>(),
                e
            )
        })
    }

    pub fn none() -> Self {
        Self::try_new(None, Nullability::Nullable).unwrap_or_else(|e| {
            unreachable!(
                "Failed to create \"None\" scalar of type {}: {}",
                any::type_name::<T>(),
                e
            )
        })
    }

    pub fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    pub fn into_value(self) -> Option<T> {
        self.value
    }

    pub fn nullability(&self) -> Nullability {
        self.nullability
    }
}
