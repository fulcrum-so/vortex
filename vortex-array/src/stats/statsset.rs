use std::collections::hash_map::{Entry, IntoIter};
use std::collections::HashMap;

use enum_iterator::all;
use itertools::Itertools;
use vortex_error::VortexError;
use vortex_scalar::{ListScalarVec, Scalar};

use crate::stats::Stat;

#[derive(Debug, Clone, Default)]
pub struct StatsSet {
    values: HashMap<Stat, Scalar>,
}

impl From<HashMap<Stat, Scalar>> for StatsSet {
    fn from(value: HashMap<Stat, Scalar>) -> Self {
        Self { values: value }
    }
}

impl StatsSet {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn of(stat: Stat, value: Scalar) -> Self {
        StatsSet::from(HashMap::from([(stat, value)]))
    }

    pub fn get(&self, stat: Stat) -> Option<&Scalar> {
        self.values.get(&stat)
    }

    fn get_as<T: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(&self, stat: Stat) -> Option<T> {
        self.get(stat).map(|v| T::try_from(v).unwrap())
    }

    pub fn set(&mut self, stat: Stat, value: Scalar) {
        self.values.insert(stat, value);
    }

    pub fn merge(&mut self, other: &Self) -> &Self {
        for s in all::<Stat>() {
            match s {
                Stat::BitWidthFreq => self.merge_bit_width_freq(other),
                Stat::TrailingZeroFreq => self.merge_trailing_zero_freq(other),
                Stat::IsConstant => self.merge_is_constant(other),
                Stat::IsSorted => self.merge_is_sorted(other),
                Stat::IsStrictSorted => self.merge_is_strict_sorted(other),
                Stat::Max => self.merge_max(other),
                Stat::Min => self.merge_min(other),
                Stat::RunCount => self.merge_run_count(other),
                Stat::TrueCount => self.merge_true_count(other),
                Stat::NullCount => self.merge_null_count(other),
            }
        }

        self
    }

    fn merge_min(&mut self, other: &Self) {
        self.merge_ordered(other, |other, own| other < own);
    }

    fn merge_max(&mut self, other: &Self) {
        self.merge_ordered(other, |other, own| other > own);
    }

    fn merge_ordered<F: Fn(&Scalar, &Scalar) -> bool>(&mut self, other: &Self, cmp: F) {
        match self.values.entry(Stat::Max) {
            Entry::Occupied(mut e) => {
                if let Some(omin) = other.get(Stat::Max) {
                    if cmp(omin, e.get()) {
                        e.insert(omin.clone());
                    }
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.get(Stat::Max) {
                    e.insert(min.clone());
                }
            }
        }
    }

    fn merge_is_constant(&mut self, other: &Self) {
        if let Some(is_constant) = self.get_as(Stat::IsConstant) {
            if let Some(other_is_constant) = other.get_as(Stat::IsConstant) {
                if is_constant && other_is_constant && self.get(Stat::Min) == other.get(Stat::Min) {
                    return;
                }
            }
            self.values.insert(Stat::IsConstant, false.into());
        }
    }

    fn merge_is_sorted(&mut self, other: &Self) {
        self.merge_sortedness_stat(other, Stat::IsSorted, |own, other| own <= other)
    }

    fn merge_is_strict_sorted(&mut self, other: &Self) {
        self.merge_sortedness_stat(other, Stat::IsStrictSorted, |own, other| own < other)
    }

    fn merge_sortedness_stat<F: Fn(Option<&Scalar>, Option<&Scalar>) -> bool>(
        &mut self,
        other: &Self,
        stat: Stat,
        cmp: F,
    ) {
        if let Some(is_sorted) = self.get_as(stat) {
            if let Some(other_is_sorted) = other.get_as(stat) {
                if is_sorted && other_is_sorted && cmp(self.get(Stat::Max), other.get(Stat::Min)) {
                    return;
                }
            }
            self.values.insert(stat, false.into());
        }
    }

    fn merge_true_count(&mut self, other: &Self) {
        self.merge_scalar_stat(other, Stat::TrueCount)
    }

    fn merge_null_count(&mut self, other: &Self) {
        self.merge_scalar_stat(other, Stat::NullCount)
    }

    fn merge_scalar_stat(&mut self, other: &Self, stat: Stat) {
        match self.values.entry(stat) {
            Entry::Occupied(mut e) => {
                if let Some(other_value) = other.get_as::<usize>(stat) {
                    let self_value: usize = e.get().try_into().unwrap();
                    e.insert((self_value + other_value).into());
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.get(stat) {
                    e.insert(min.clone());
                }
            }
        }
    }

    fn merge_bit_width_freq(&mut self, other: &Self) {
        self.merge_freq_stat(other, Stat::BitWidthFreq)
    }

    fn merge_trailing_zero_freq(&mut self, other: &Self) {
        self.merge_freq_stat(other, Stat::TrailingZeroFreq)
    }

    fn merge_freq_stat(&mut self, other: &Self, stat: Stat) {
        match self.values.entry(stat) {
            Entry::Occupied(mut e) => {
                if let Some(other_value) = other.get_as::<ListScalarVec<u64>>(stat) {
                    // TODO(robert): Avoid the copy here. We could e.get_mut() but need to figure out casting
                    let self_value: ListScalarVec<u64> = e.get().try_into().unwrap();
                    e.insert(
                        ListScalarVec(
                            self_value
                                .0
                                .iter()
                                .zip_eq(other_value.0.iter())
                                .map(|(s, o)| *s + *o)
                                .collect::<Vec<_>>(),
                        )
                        .into(),
                    );
                }
            }
            Entry::Vacant(e) => {
                if let Some(other_value) = other.get(stat) {
                    e.insert(other_value.clone());
                }
            }
        }
    }

    /// Merged run count is an upper bound where we assume run is interrupted at the boundary
    fn merge_run_count(&mut self, other: &Self) {
        match self.values.entry(Stat::RunCount) {
            Entry::Occupied(mut e) => {
                if let Some(other_value) = other.get_as::<usize>(Stat::RunCount) {
                    let self_value: usize = e.get().try_into().unwrap();
                    e.insert((self_value + other_value + 1).into());
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.get(Stat::RunCount) {
                    e.insert(min.clone());
                }
            }
        }
    }
}

impl Extend<(Stat, Scalar)> for StatsSet {
    #[inline]
    fn extend<T: IntoIterator<Item = (Stat, Scalar)>>(&mut self, iter: T) {
        self.values.extend(iter)
    }
}

impl IntoIterator for StatsSet {
    type Item = (Stat, Scalar);
    type IntoIter = IntoIter<Stat, Scalar>;

    fn into_iter(self) -> IntoIter<Stat, Scalar> {
        self.values.into_iter()
    }
}
