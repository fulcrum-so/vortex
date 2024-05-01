use serde::{Deserialize, Serialize};
use vortex_dtype::FieldNames;
use vortex_error::vortex_bail;

use crate::stats::ArrayStatisticsCompute;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayDType};
use crate::{ArrayFlatten, IntoArrayData};

mod compute;

impl_encoding!("vortex.struct", Struct);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructMetadata {
    length: usize,
}

impl StructArray<'_> {
    pub fn child(&self, idx: usize) -> Option<Array> {
        let DType::Struct(_, fields) = self.dtype() else {
            unreachable!()
        };
        let dtype = fields.get(idx)?;
        self.array().child(idx, dtype)
    }

    pub fn names(&self) -> &FieldNames {
        let DType::Struct(names, _fields) = self.dtype() else {
            unreachable!()
        };
        names
    }

    pub fn fields(&self) -> &[DType] {
        let DType::Struct(_names, fields) = self.dtype() else {
            unreachable!()
        };
        fields.as_slice()
    }

    pub fn nfields(&self) -> usize {
        self.fields().len()
    }
}

impl<'a> StructArray<'a> {
    pub fn children(&'a self) -> impl Iterator<Item = Array<'a>> {
        (0..self.nfields()).map(move |idx| self.child(idx).unwrap())
    }
}

impl StructArray<'_> {
    pub fn try_new(names: FieldNames, fields: Vec<Array>, length: usize) -> VortexResult<Self> {
        if names.len() != fields.len() {
            vortex_bail!("Got {} names and {} fields", names.len(), fields.len());
        }

        if fields.iter().any(|a| a.with_dyn(|a| a.len()) != length) {
            vortex_bail!("Expected all struct fields to have length {}", length);
        }

        let field_dtypes: Vec<_> = fields.iter().map(|d| d.dtype()).cloned().collect();
        Self::try_from_parts(
            DType::Struct(names, field_dtypes),
            StructMetadata { length },
            fields.into_iter().map(|a| a.into_array_data()).collect(),
            StatsSet::new(),
        )
    }
}

impl ArrayFlatten for StructArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        Ok(Flattened::Struct(StructArray::try_new(
            self.names().clone(),
            (0..self.nfields())
                .map(|i| {
                    self.child(i)
                        .expect("Missing child")
                        .flatten()
                        .map(|f| f.into_array())
                })
                .collect::<VortexResult<Vec<_>>>()?,
            self.len(),
        )?))
    }
}

impl ArrayTrait for StructArray<'_> {
    fn len(&self) -> usize {
        self.metadata().length
    }
}

impl ArrayValidity for StructArray<'_> {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }

    fn logical_validity(&self) -> LogicalValidity {
        todo!()
    }
}

impl AcceptArrayVisitor for StructArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        for (idx, name) in self.names().iter().enumerate() {
            let child = self.child(idx).unwrap();
            visitor.visit_child(&format!("\"{}\"", name), &child)?;
        }
        Ok(())
    }
}

impl ArrayStatisticsCompute for StructArray<'_> {}

impl EncodingCompression for StructEncoding {}
