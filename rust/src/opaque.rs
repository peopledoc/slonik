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
