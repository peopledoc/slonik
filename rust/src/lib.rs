extern crate postgres;

use std::os::raw::c_char;
use std::slice;
use std::str;
use postgres::{Connection, TlsMode};
use postgres::rows::{Rows};


#[no_mangle]
pub struct _Connection;

#[no_mangle]
pub struct _Rows;


#[no_mangle]
pub unsafe extern "C" fn connect(dsn: *const c_char, len: usize) -> *mut _Connection {
    let dsn_str = str::from_utf8_unchecked(slice::from_raw_parts(dsn as *const _, len));
    let conn = Connection::connect(dsn_str, TlsMode::None).unwrap();
    let ptr = Box::new(conn);
    Box::into_raw(ptr) as *mut _Connection
}


#[no_mangle]
pub unsafe extern "C" fn query(conn: *mut _Connection, query: *const c_char, len: usize) -> *mut _Rows {
    let conn = conn as *mut Connection;
    let query_str = str::from_utf8_unchecked(slice::from_raw_parts(query as *const _, len));
    let ptr = Box::new((*conn).query(query_str, &[]).unwrap());
    Box::into_raw(ptr) as *mut _Rows
}


#[no_mangle]
pub unsafe extern "C" fn next_row<'a>(rows: *mut _Rows) -> i32 {
    let rows = rows as *mut Rows;
    let mut iter = (*rows).iter();
    iter.next().unwrap().get(0)
}
