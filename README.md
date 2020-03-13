# Slonik - An experiment from the past for the Python PostgreSQL driver of the Future

[![CircleCI](https://circleci.com/gh/peopledoc/slonik.svg?style=shield)](https://circleci.com/gh/peopledoc/slonik)

Rust Inside™

A Python binding to [`rust-postgres`](https://github.com/sfackler/rust-postgres), a native PostgreSQL driver for Rust.

IRC channel: [#slonik](https://webchat.freenode.net/?channels=slonik) on [freenode](https://freenode.net/).

**Note: Slonik started as an experiment, and will probably never go past that.** We've learned tons of things during its creation, two of them being that programs using CFFI are bound to be slower (with CPython) than implementations using the Python C API, and that interfacing the Python Event Loop with the async support of Rust is… challenging at best.
