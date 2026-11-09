//! wRPC encoding and decoding of values implementing [`wasm_wave`] traits
//!
//! Values are encoded with [`ValueEncoder`] and decoded with [`decode_value`]. With the default
//! `transport` feature, they are also encoded with `wrpc_transport::Encode` when wrapped in a
//! `TypedValue`, and read with `read_value`.
//!
//! ```
//! use bytes::BytesMut;
//! use wasm_wave::value::{Type, Value};
//! use wasm_wave::wasm::WasmValue as _;
//! use wrpc_wave::{ValueEncoder, decode_value};
//!
//! let ty = Type::list(Type::U32);
//! let v = Value::make_list(&ty, [Value::make_u32(1), Value::make_u32(2)]).unwrap();
//! let mut buf = BytesMut::default();
//! ValueEncoder::new(&ty).encode(&v, &mut buf).unwrap();
//! assert_eq!(decode_value::<Value>(&buf, &ty).unwrap(), v);
//! ```

mod decode;
mod encode;
#[cfg(feature = "transport")]
mod typed;

pub use decode::decode_value;
#[cfg(feature = "transport")]
pub use decode::read_value;
pub use encode::ValueEncoder;
#[cfg(feature = "transport")]
pub use typed::{TypedValue, TypedValueEncoder};
