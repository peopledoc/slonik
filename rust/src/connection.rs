extern crate postgres;

use std::os::raw::c_char;
use std::slice;
use std::str;

pub use postgres::{Connection, TlsMode};
use result::*;
use opaque::*;


#[no_mangle]
pub struct _Connection;

#[no_mangle]
pub struct _Query;


#[no_mangle]
pub unsafe extern "C" fn connect(dsn: *const c_char, len: usize) -> FFIResult<_Connection> {
    let dsn_str = str::from_utf8_unchecked(slice::from_raw_parts(dsn as *const _, len));
    FFIResult::from_result(Connection::connect(dsn_str, TlsMode::None))
}


#[no_mangle]
pub unsafe extern "C" fn close(conn: *mut _Connection) {
    let conn = OpaquePtr::<Connection>::from_opaque(conn);
    conn.free();
}
