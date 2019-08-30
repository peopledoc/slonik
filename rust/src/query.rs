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

pub trait ParamType {
    const name: &'static str;
}

macro_rules! get_typed_param {
    ($typename: expr, $value: expr) => {
        {
            #[derive(Copy, Clone, Debug)]
            struct _ParamType {}
            impl ParamType for _ParamType {
                const name: &'static str = $typename;
            }
            Box::new(TypedQueryParam::<_ParamType>::new($value))
        }
    }
}

impl QueryParam {
    pub unsafe fn typed_param(&self) -> Box<postgres::types::ToSql> {
        match self.typename.to_str() {
            "text" => get_typed_param!("text", self.value),
            "int4" => get_typed_param!("int4", self.value),
            "float8" => get_typed_param!("float8", self.value),
            _ => {
                println!("unknown type: {:?}", self.typename.to_str());
                get_typed_param!("", self.value)
            },
        }
    }
}

#[no_mangle]
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct TypedQueryParam<T: ParamType + std::fmt::Debug> {
    pub value: Buffer,
    phantom: std::marker::PhantomData<T>,
}
impl<T: ParamType + std::fmt::Debug> TypedQueryParam<T> {
    pub fn new(value: Buffer) -> Self {
        Self{value, phantom: std::marker::PhantomData}
    }
}
impl<T: ParamType + std::fmt::Debug> postgres::types::ToSql for TypedQueryParam<T> {
    fn to_sql(&self, ty: &postgres::types::Type, out: &mut Vec<u8>) -> Result<postgres::types::IsNull, Box<std::error::Error + 'static + Send + Sync>> {
        for i in 0..self.value.size {
            out.push(unsafe { *self.value.bytes.offset(i as isize) });
        }
        Ok(postgres::types::IsNull::No)
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        ty.name() == T::name
    }

    postgres::to_sql_checked!();
}

pub struct Query<'a> {
    pub conn: &'a Connection,
    pub query: String,
    pub params: Vec<Box<postgres::types::ToSql>>,
}

impl<'a> Query<'a> {
    pub fn sql_params(&self) -> Vec<&postgres::types::ToSql> {
        self.params.iter().map(|p| p.as_ref()).collect()
    }

    pub fn execute(&self) -> Result<u64, postgres::Error> {
        let params = self.sql_params();
        self.conn.execute(&self.query, params.as_slice())
    }

    pub fn execute_with_result(&self) -> Result<QueryResult, postgres::Error> {
        let params = self.sql_params();
        let result = self.conn.query(&self.query, params.as_slice());
        result.map(|rows| QueryResult::from_rows(rows))
    }
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
    query.params.push(param.typed_param());
}


#[no_mangle]
pub unsafe extern "C" fn query_exec(query: *mut _Query) -> FFIResult<u8> {
    let query = OpaquePtr::<Query>::from_opaque(query);
    let result = query.execute();
    query.free();
    FFIResult::from_result(result)
}


#[no_mangle]
pub unsafe extern "C" fn query_exec_result(query: *mut _Query) -> FFIResult<_QueryResult> {
    let query = OpaquePtr::<Query>::from_opaque(query);
    let result = query.execute_with_result();
    query.free();
    FFIResult::from_result(result)
}


#[no_mangle]
pub unsafe extern "C" fn result_close(result: *mut _QueryResult) {
    let result = OpaquePtr::<QueryResult>::from_opaque(result);
    result.free();
}
