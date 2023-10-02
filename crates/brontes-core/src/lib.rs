pub mod decoding;
pub mod errors;
pub mod executor;
pub mod macros;

use alloy_sol_types::{sol, SolInterface};

include!(concat!(env!("OUT_DIR"), "/protocol_addr_set.rs"));
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub trait TryDecodeSol {
    type DecodingType;

    fn try_decode(call_data: &[u8]) -> Result<Self::DecodingType, alloy_sol_types::Error>;
}

/// implements the above trait for decoding on the different binding enums
#[macro_export]
macro_rules! impl_decode_sol {
    ($enum_name:ident, $inner_type:path ) => {
        impl TryDecodeSol for $enum_name {
            type DecodingType = $inner_type;

            fn try_decode(call_data: &[u8]) -> Result<Self::DecodingType, alloy_sol_types::Error> {
                Self::DecodingType::abi_decode(call_data, true)
            }
        }
    };
}