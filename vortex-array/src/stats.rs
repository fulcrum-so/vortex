use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::dtype::DType;

use crate::error::{VortexError, VortexResult};
use crate::ptype::NativePType;
use crate::scalar::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Stat {
    BitWidthFreq,
    TZFreq,
    IsConstant,
    IsSorted,
    IsStrictSorted,
    Max,
    Min,
    RunCount,
    TrueCount,
    NullCount,
}

#[derive(Debug, Clone, Default)]
pub struct StatsSet(HashMap<Stat, ScalarRef>);

impl StatsSet {
    pub fn new() -> Self {
        StatsSet(HashMap::new())
    }

    pub fn from(map: HashMap<Stat, ScalarRef>) -> Self {
        StatsSet(map)
    }

    pub fn of(stat: Stat, value: ScalarRef) -> Self {
        StatsSet(HashMap::from([(stat, value)]))
    }

    fn get_as<T: TryFrom<ScalarRef, Error = VortexError>>(
        &self,
        stat: &Stat,
    ) -> VortexResult<Option<T>> {
        self.0.get(stat).map(|v| T::try_from(v.clone())).transpose()
    }

    pub fn set(&mut self, stat: Stat, value: ScalarRef) {
        self.0.insert(stat, value);
    }

    pub fn merge(&mut self, other: &Self) -> &Self {
        // FIXME(ngates): make adding a new stat a compile error
        self.merge_min(other);
        self.merge_max(other);
        self.merge_is_constant(other);
        self.merge_is_sorted(other);
        self.merge_true_count(other);
        self.merge_null_count(other);
        // self.merge_bit_width_freq(other);
        self.merge_run_count(other);

        self
    }

    fn merge_min(&mut self, other: &Self) {
        match self.0.entry(Stat::Min) {
            Entry::Occupied(mut e) => {
                if let Some(omin) = other.0.get(&Stat::Min) {
                    match omin.partial_cmp(e.get().as_ref()) {
                        None => {
                            e.remove();
                        }
                        Some(Ordering::Less) => {
                            e.insert(omin.clone());
                        }
                        Some(Ordering::Equal) | Some(Ordering::Greater) => {}
                    }
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.0.get(&Stat::Min) {
                    e.insert(min.clone());
                }
            }
        }
    }

    fn merge_max(&mut self, other: &Self) {
        match self.0.entry(Stat::Max) {
            Entry::Occupied(mut e) => {
                if let Some(omin) = other.0.get(&Stat::Max) {
                    match omin.partial_cmp(e.get().as_ref()) {
                        None => {
                            e.remove();
                        }
                        Some(Ordering::Greater) => {
                            e.insert(omin.clone());
                        }
                        Some(Ordering::Equal) | Some(Ordering::Less) => {}
                    }
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.0.get(&Stat::Max) {
                    e.insert(min.clone());
                }
            }
        }
    }

    fn merge_is_constant(&mut self, other: &Self) {
        if let Some(is_constant) = self.get_as::<bool>(&Stat::IsConstant).unwrap() {
            if let Some(other_is_constant) = other.get_as::<bool>(&Stat::IsConstant).unwrap() {
                if is_constant
                    && other_is_constant
                    && self.0.get(&Stat::Min) == other.0.get(&Stat::Min)
                {
                    return;
                }
            }
            self.0.insert(Stat::IsConstant, false.into());
        }
    }

    fn merge_is_sorted(&mut self, other: &Self) {
        if let Some(is_sorted) = self.get_as::<bool>(&Stat::IsSorted).unwrap() {
            if let Some(other_is_sorted) = other.get_as::<bool>(&Stat::IsSorted).unwrap() {
                if is_sorted && other_is_sorted && self.0.get(&Stat::Max) <= other.0.get(&Stat::Min)
                {
                    return;
                }
            }
            self.0.insert(Stat::IsSorted, false.into());
        }
    }

    fn merge_true_count(&mut self, other: &Self) {
        self.merge_scalar_stat(other, &Stat::TrueCount)
    }

    fn merge_null_count(&mut self, other: &Self) {
        self.merge_scalar_stat(other, &Stat::NullCount)
    }

    fn merge_scalar_stat(&mut self, other: &Self, stat: &Stat) {
        match self.0.entry(stat.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(other_value) = other.get_as::<usize>(stat).unwrap() {
                    let self_value: usize = e.get().as_ref().try_into().unwrap();
                    e.insert((self_value + other_value).into());
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.0.get(stat) {
                    e.insert(min.clone());
                }
            }
        }
    }

    /// Merged run count is an upper bound where we assume run is interrupted at the boundary
    fn merge_run_count(&mut self, other: &Self) {
        match self.0.entry(Stat::RunCount) {
            Entry::Occupied(mut e) => {
                if let Some(other_value) = other.get_as::<usize>(&Stat::RunCount).unwrap() {
                    let self_value: usize = e.get().as_ref().try_into().unwrap();
                    e.insert((self_value + other_value + 1).into());
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.0.get(&Stat::RunCount) {
                    e.insert(min.clone());
                }
            }
        }
    }
}

pub trait StatsCompute {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::new())
    }
}

pub struct Stats<'a> {
    cache: &'a RwLock<StatsSet>,
    compute: &'a dyn StatsCompute,
}

impl<'a> Stats<'a> {
    pub fn new(cache: &'a RwLock<StatsSet>, compute: &'a dyn StatsCompute) -> Self {
        Self { cache, compute }
    }

    pub fn set_many(&self, other: &Stats, stats: Vec<&Stat>) {
        stats.into_iter().for_each(|stat| {
            if let Some(v) = other.get(stat) {
                self.cache.write().unwrap().set(stat.clone(), v)
            }
        });
    }

    pub fn set(&self, stat: Stat, value: ScalarRef) {
        self.cache.write().unwrap().set(stat, value);
    }

    pub fn get_all(&self) -> StatsSet {
        self.cache.read().unwrap().clone()
    }

    pub fn get(&self, stat: &Stat) -> Option<ScalarRef> {
        self.cache.read().unwrap().0.get(stat).cloned()
    }

    pub fn get_as<T: TryFrom<ScalarRef, Error = VortexError>>(&self, stat: &Stat) -> Option<T> {
        self.get(stat).map(|v| T::try_from(v).unwrap())
    }

    pub fn get_or_compute(&self, stat: &Stat) -> Option<ScalarRef> {
        if let Some(value) = self.cache.read().unwrap().0.get(stat) {
            return Some(value.clone());
        }

        self.cache
            .write()
            .unwrap()
            .0
            .extend(self.compute.compute(stat).unwrap().0);
        self.get(stat)
    }

    pub fn get_or_compute_cast<T: NativePType>(&self, stat: &Stat) -> Option<T> {
        self.get_or_compute(stat)
            // TODO(ngates): fix the API so we don't convert the result to optional
            .and_then(|v: ScalarRef| v.cast(&DType::from(T::PTYPE)).ok())
            .and_then(|v| T::try_from(v).ok())
    }

    pub fn get_or_compute_as<T: TryFrom<ScalarRef, Error = VortexError>>(
        &self,
        stat: &Stat,
    ) -> Option<T> {
        self.get_or_compute(stat).and_then(|v| T::try_from(v).ok())
    }

    pub fn get_or_compute_or<T: TryFrom<ScalarRef, Error = VortexError>>(
        &self,
        default: T,
        stat: &Stat,
    ) -> T {
        self.get_or_compute_as(stat).unwrap_or(default)
    }
}
