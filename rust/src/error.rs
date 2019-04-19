use opaque::*;
use buffer::*;

pub struct Error {
    pub code: u8,
    pub msg: String,
}

#[no_mangle]
pub struct _Error;


#[no_mangle]
pub unsafe extern "C" fn error_msg(error: *mut _Error) -> Buffer {
    let error = OpaquePtr::<Error>::from_opaque(error);
    Buffer::from_str(&error.msg)
}

#[no_mangle]
pub unsafe extern "C" fn error_free(error: *mut _Error) {
    let error = OpaquePtr::<Error>::from_opaque(error);
    error.free();
}
