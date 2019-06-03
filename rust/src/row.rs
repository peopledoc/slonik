use buffer::*;
use opaque::*;
use query::*;


pub struct _Rows;
pub struct _RowsIterator;


#[no_mangle]
pub struct _Row;

#[no_mangle]
pub struct _Opaque;


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
