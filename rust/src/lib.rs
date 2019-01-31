extern crate postgres;

use std::os::raw::c_char;
use std::slice;
use std::str;
use postgres::{Connection, TlsMode};
use postgres::rows::{Rows};


#[no_mangle]
pub struct _Connection;

#[no_mangle]
pub struct _Query;

#[no_mangle]
pub struct _Rows;

#[no_mangle]
pub struct _RowsIterator;

#[no_mangle]
pub struct _Row;

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

pub struct Query {
    pub conn: *mut Connection,
    pub query: String,
    pub params: Vec<QueryParam>,
}


#[no_mangle]
pub unsafe extern "C" fn connect(dsn: *const c_char, len: usize) -> *mut _Connection {
    let dsn_str = str::from_utf8_unchecked(slice::from_raw_parts(dsn as *const _, len));
    let conn = Connection::connect(dsn_str, TlsMode::None).unwrap();
    let ptr = Box::new(conn);
    Box::into_raw(ptr) as *mut _Connection
}


#[no_mangle]
pub unsafe extern "C" fn new_query(conn: *mut _Connection, query: *const c_char, len: usize) -> *mut _Query {
    let conn = conn as *mut Connection;
    let query_str = str::from_utf8_unchecked(slice::from_raw_parts(query as *const _, len));
    let q = Query { conn: conn, query: query_str.to_string(), params: vec![] };
    let ptr = Box::new(q);
    Box::into_raw(ptr) as *mut _Query
}


#[no_mangle]
pub unsafe extern "C" fn query_param(query: *mut _Query, param: QueryParam) {
    let query = &mut *(query as *mut Query);
    query.params.push(param);
}


#[no_mangle]
pub unsafe extern "C" fn query_exec(query: *const _Query) {
    let query = &*(query as *const Query);
    let conn = &*query.conn;
    let mut params: Vec<&postgres::types::ToSql> = vec![];
    for param in &query.params {
        params.push(*Box::new(param));
    }
    conn.execute(&query.query, params.as_slice());
}


#[no_mangle]
pub unsafe extern "C" fn query_exec_result(query: *const _Query) -> *mut _Rows {
    let query = &*(query as *const Query);
    let conn = &*query.conn;
    let mut params: Vec<&postgres::types::ToSql> = vec![];
    for param in &query.params {
        params.push(*Box::new(param));
    }
    let ptr = Box::new(conn.query(&query.query, params.as_slice()).unwrap());
    Box::into_raw(ptr) as *mut _Rows
}


#[no_mangle]
pub unsafe extern "C" fn rows_iterator(rows: *mut _Rows) -> *mut _RowsIterator {
    let rows = rows as *mut Rows;
    let iter = (*rows).iter();
    let ptr = Box::new(iter);
    Box::into_raw(ptr) as *mut _RowsIterator
}


#[no_mangle]
pub unsafe extern "C" fn next_row(iter: *mut _RowsIterator) -> *const _Row {
    let iter = iter as *mut postgres::rows::Iter;
    match (*iter).next() {
        Some(x) => {
            let ptr = Box::new(x);
            Box::into_raw(ptr) as *const _Row
        }
        None => std::ptr::null(),
    }
}


#[no_mangle]
pub unsafe extern "C" fn row_len(row: *const _Row) -> usize {
    let row = row as *const postgres::rows::Row;
    (*row).len()
}


#[no_mangle]
pub unsafe extern "C" fn row_item(row: *const _Row, i: usize) -> RowItem {
    let row = row as *const postgres::rows::Row;
    let typename = (*row).columns()[i].type_().name();

    match (*row).get_bytes(i) {
        Some(data) => RowItem{
            typename: Buffer::from_str(typename),
            value: Buffer::from_bytes(data),
        },
        None => RowItem::empty(),
    }
}
