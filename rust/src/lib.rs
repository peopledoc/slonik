extern crate postgres;

use std::os::raw::c_char;
use std::slice;
use std::str;
use postgres::{Connection, TlsMode};


#[no_mangle]
pub struct _Connection;

#[no_mangle]
pub struct _Query;

struct _Rows;
struct _RowsIterator;

#[no_mangle]
pub struct _QueryResult;

#[no_mangle]
pub struct _Row;

#[no_mangle]
pub struct _Opaque;

#[no_mangle]
pub struct _Error;

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

pub struct Error {
    code: u8,
    msg: String,
}

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

pub struct OpaquePtr<T> {
    ptr: *mut T
}
impl<T> OpaquePtr<T> {
    pub fn from_ptr(ptr: *mut T) -> Self {
        Self { ptr }
    }
    pub fn from_box(boxed_value: Box<T>) -> Self {
        Self::from_ptr(Box::into_raw(boxed_value))
    }
    pub fn new(value: T) -> Self {
        Self::from_box(Box::new(value))
    }
    pub unsafe fn free(&self) {
        Box::from_raw(self.ptr);
    }

    pub fn as_ptr(&self) -> *mut T {
        self.ptr
    }
    pub fn as_ref(&self) -> &mut T {
        unsafe { &mut *self.ptr }
    }

    pub fn from_opaque<O>(opaque: *mut O) -> Self {
        Self::from_ptr(opaque as *mut T)
    }
    pub fn opaque<O>(&self) -> *mut O {
        self.ptr as *mut O
    }
}

impl<T> std::ops::Deref for OpaquePtr<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.as_ref()
    }
}

impl<T> std::ops::DerefMut for OpaquePtr<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.as_ref()
    }
}


#[no_mangle]
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct Buffer {
    pub size: usize,
    pub bytes: *const u8,
}

impl Buffer {
    pub fn null() -> Self {
        Self{size: 0, bytes: std::ptr::null()}
    }
    pub fn from_bytes(data: &[u8]) -> Self {
        Self{size: data.len(), bytes: data.as_ptr()}
    }
    pub fn from_str(data: &str) -> Self {
        Self::from_bytes(data.as_bytes())
    }
    pub unsafe fn to_str(&self) -> &str {
        str::from_utf8_unchecked(slice::from_raw_parts(self.bytes as *const _, self.size))
    }
}

#[no_mangle]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct RowItem {
    pub typename: Buffer,
    pub value: Buffer,
}

impl RowItem {
    pub fn empty() -> Self {
        let buff = Buffer::null();
        Self{typename: buff, value: buff}
    }
}

#[no_mangle]
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct QueryParam {
    pub typename: Buffer,
    pub value: Buffer,
}

impl postgres::types::ToSql for QueryParam {
    fn to_sql(&self, ty: &postgres::types::Type, out: &mut Vec<u8>) -> Result<postgres::types::IsNull, Box<std::error::Error + 'static + Send + Sync>> {
        for i in 0..self.value.size {
            out.push(unsafe { *self.value.bytes.offset(i as isize) });
        }
        Ok(postgres::types::IsNull::No)
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        //ty.name() == "int4"
        true
    }

    postgres::to_sql_checked!();
}

pub struct Query<'a> {
    pub conn: &'a Connection,
    pub query: String,
    pub params: Vec<QueryParam>,
}


struct QueryResult {
    rows: *mut _Rows,
    iter: *mut _RowsIterator,
}

impl QueryResult {
    pub fn from_rows(rows: postgres::rows::Rows) -> Self {
        let iter = OpaquePtr::new(rows.iter()).opaque();
        let rows = OpaquePtr::new(rows).opaque();
        Self{rows, iter}
    }
}
impl Drop for QueryResult {
    fn drop (&mut self) {
        let rows = OpaquePtr::<postgres::rows::Rows>::from_opaque(self.rows);
        let iter = OpaquePtr::<postgres::rows::Iter>::from_opaque(self.iter);
        unsafe {
            iter.free();
            rows.free();
        }
    }
}


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


#[no_mangle]
pub unsafe extern "C" fn new_query(conn: *mut _Connection, query: *const c_char, len: usize) -> *mut _Query {
    let conn = OpaquePtr::<Connection>::from_opaque(conn);
    let query_str = str::from_utf8_unchecked(slice::from_raw_parts(query as *const _, len));
    let q = Query { conn: &conn, query: query_str.to_string(), params: vec![] };
    OpaquePtr::new(q).opaque()
}


#[no_mangle]
pub unsafe extern "C" fn query_param(query: *mut _Query, param: QueryParam) {
    let query = &mut *(query as *mut Query);
    query.params.push(param);
}


fn get_query_params<'a>(query: &'a Query) -> Vec<&'a postgres::types::ToSql> {
    let mut params: Vec<&postgres::types::ToSql> = vec![];
    for param in &query.params {
        params.push(*Box::new(param));
    }
    params
}


#[no_mangle]
pub unsafe extern "C" fn query_exec(query: *mut _Query) -> FFIResult<u8> {
    let query = OpaquePtr::<Query>::from_opaque(query);
    let params = get_query_params(&query);
    let result = query.conn.execute(&query.query, params.as_slice());
    query.free();
    FFIResult::from_result(result)
}


#[no_mangle]
pub unsafe extern "C" fn query_exec_result(query: *mut _Query) -> FFIResult<_QueryResult> {
    let query = OpaquePtr::<Query>::from_opaque(query);
    let params = get_query_params(&query);
    let result = query.conn.query(&query.query, params.as_slice());
    query.free();

    FFIResult::from_result(result.map(|r| QueryResult::from_rows(r)))
}


#[no_mangle]
pub unsafe extern "C" fn result_close(result: *mut _QueryResult) {
    let result = OpaquePtr::<QueryResult>::from_opaque(result);
    result.free();
}


#[no_mangle]
pub unsafe extern "C" fn next_row(result: *mut _QueryResult) -> *const _Row {
    let result = OpaquePtr::<QueryResult>::from_opaque(result);
    let mut iter = OpaquePtr::<postgres::rows::Iter>::from_opaque(result.iter);
    match iter.next() {
        Some(x) => {
            OpaquePtr::new(x).opaque()
        }
        None => std::ptr::null(),
    }
}


#[no_mangle]
pub unsafe extern "C" fn row_len(row: *mut _Row) -> usize {
    let row = OpaquePtr::<postgres::rows::Row>::from_opaque(row);
    row.len()
}


#[no_mangle]
pub unsafe extern "C" fn row_close(row: *mut _Row) {
    let row = OpaquePtr::<postgres::rows::Row>::from_opaque(row);
    row.free();
}


#[no_mangle]
pub unsafe extern "C" fn row_item(row: *mut _Row, i: usize) -> RowItem {
    let row = OpaquePtr::<postgres::rows::Row>::from_opaque(row);
    let typename = row.columns()[i].type_().name();

    match row.get_bytes(i) {
        Some(data) => RowItem{
            typename: Buffer::from_str(typename),
            value: Buffer::from_bytes(data),
        },
        None => RowItem::empty(),
    }
}
