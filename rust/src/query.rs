use std::os::raw::c_char;
use std::slice;
use std::str;

use opaque::*;
use result::*;
use buffer::*;
use connection::*;
use row::*;


#[no_mangle]
pub struct _Query;

#[no_mangle]
pub struct _QueryResult;


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


pub struct QueryResult {
    pub rows: *mut _Rows,
    pub iter: *mut _RowsIterator,
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
