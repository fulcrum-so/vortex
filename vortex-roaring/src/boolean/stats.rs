use vortex::array::Array;
use vortex::stats::{Stat, StatsCompute, StatsSet};
use vortex_error::VortexResult;

use crate::boolean::RoaringBoolArray;

impl StatsCompute for RoaringBoolArray {
    fn compute(&self, stat: &Stat) -> VortexResult<StatsSet> {
        let cardinality = self.bitmap().cardinality() as usize;
        if let Some(value) = match stat {
            Stat::IsConstant => Some((cardinality == self.len() || cardinality == 0).into()),
            Stat::Max => {
                if self.len() > 0 {
                    Some((cardinality > 0).into())
                } else {
                    None
                }
            }
            Stat::Min => {
                if self.len() > 0 {
                    Some((cardinality == self.len()).into())
                } else {
                    None
                }
            }
            Stat::TrueCount => Some(cardinality.into()),
            Stat::NullCount => Some(0.into()),
            _ => None,
        } {
            Ok(StatsSet::of(stat.clone(), value))
        } else {
            Ok(StatsSet::default())
        }
    }
}

#[cfg(test)]
mod test {
    use vortex::array::bool::BoolArray;
    use vortex::array::Array;
    use vortex::stats::Stat::*;
    use vortex_error::VortexResult;

    use crate::RoaringBoolArray;

    #[test]
    pub fn stats_all_true() -> VortexResult<()> {
        let bool: &dyn Array = &BoolArray::from(vec![true, true]);
        let array = RoaringBoolArray::encode(bool)?;

        assert_eq!(
            array.stats().get_or_compute_as::<bool>(&IsConstant),
            Some(true)
        );
        assert_eq!(array.stats().get_or_compute_as::<bool>(&Min), Some(true));
        assert_eq!(array.stats().get_or_compute_as::<bool>(&Max), Some(true));
        assert_eq!(
            array
                .stats()
                .get_or_compute_cast::<u32>(&TrueCount)
                .unwrap(),
            2
        );

        Ok(())
    }

    #[test]
    pub fn stats_all_false() -> VortexResult<()> {
        let bool: &dyn Array = &BoolArray::from(vec![false, false]);
        let array = RoaringBoolArray::encode(bool)?;

        assert_eq!(
            array.stats().get_or_compute_as::<bool>(&IsConstant),
            Some(true)
        );
        assert_eq!(array.stats().get_or_compute_as::<bool>(&Min), Some(false));
        assert_eq!(array.stats().get_or_compute_as::<bool>(&Max), Some(false));
        assert_eq!(
            array
                .stats()
                .get_or_compute_cast::<u32>(&TrueCount)
                .unwrap(),
            0
        );

        Ok(())
    }

    #[test]
    pub fn stats_mixed() -> VortexResult<()> {
        let bool: &dyn Array = &BoolArray::from(vec![false, true, true]);
        let array = RoaringBoolArray::encode(bool)?;

        assert_eq!(
            array.stats().get_or_compute_as::<bool>(&IsConstant),
            Some(false)
        );
        assert_eq!(array.stats().get_or_compute_as::<bool>(&Min), Some(false));
        assert_eq!(array.stats().get_or_compute_as::<bool>(&Max), Some(true));
        assert_eq!(
            array
                .stats()
                .get_or_compute_cast::<u32>(&TrueCount)
                .unwrap(),
            2
        );

        Ok(())
    }
}
