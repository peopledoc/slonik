use opaque::*;
use error::*;

#[no_mangle]
#[repr(C)]
pub struct FFIResult<T> {
    status: u8,
    data: *const T,
}

impl<T> FFIResult<T> {
    fn new<O>(status: u8, obj: O) -> Self {
        Self { status, data: OpaquePtr::new(obj).opaque() }
    }
    pub fn from_obj<O>(obj: O) -> Self {
        Self::new(0, obj)
    }
    pub fn from_error<E: std::error::Error>(error: E) -> Self {
        let error = Error { code: 1, msg: format!("{}", error) };
        Self::new(error.code, error)
    }
    pub fn from_result<O, E: std::error::Error>(result: Result<O, E>) -> Self {
        match result {
            Ok(o) => Self::from_obj(o),
            Err(e) => Self::from_error(e),
        }
    }
}
