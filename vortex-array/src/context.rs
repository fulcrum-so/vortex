use std::collections::HashMap;

use crate::array::bool::BoolEncoding;
use crate::array::chunked::ChunkedEncoding;
use crate::array::constant::ConstantEncoding;
use crate::array::extension::ExtensionEncoding;
use crate::array::primitive::PrimitiveEncoding;
use crate::array::r#struct::StructEncoding;
use crate::array::sparse::SparseEncoding;
use crate::array::varbin::VarBinEncoding;
use crate::array::varbinview::VarBinViewEncoding;
use crate::encoding::EncodingRef;

#[derive(Debug, Clone)]
pub struct Context {
    encodings: HashMap<String, EncodingRef>,
}

impl Context {
    pub fn with_encoding(mut self, encoding: EncodingRef) -> Self {
        self.encodings.insert(encoding.id().to_string(), encoding);
        self
    }

    pub fn encodings(&self) -> impl Iterator<Item = EncodingRef> + '_ {
        self.encodings.values().cloned()
    }

    pub fn lookup_encoding(&self, encoding_id: &str) -> Option<EncodingRef> {
        self.encodings.get(encoding_id).cloned()
    }
}

impl Default for Context {
    fn default() -> Self {
        Context {
            encodings: HashMap::from_iter(
                [
                    &BoolEncoding as EncodingRef,
                    &ChunkedEncoding,
                    &ConstantEncoding,
                    &ExtensionEncoding,
                    &PrimitiveEncoding,
                    &SparseEncoding,
                    &StructEncoding,
                    &VarBinEncoding,
                    &VarBinViewEncoding,
                ]
                .iter()
                .map(|e| (e.id().to_string(), *e)),
            ),
        }
    }
}
