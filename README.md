tivan
=====

A wrapper for mnesia to use maps and map functions for the CRUD database operations and complex queries in place of records and qlc.

Some of the functionality of the wrapper are -
- Table definitions using maps instead of records
- Reading and writing rows as maps
- Writing multiple rows with a single function call
- Writing a row returns the primary key of the row
- Global configuration for the context with a context override for each request
- Ability to describe match specs elegantly through maps
- Row wise full text search
- Pagination through select count and ets cache
- Sorting of rows in the ets cache
- A server behaviour for CRUD operations with data type, limit and unique creteria validations

Build
-----
    $ rebar3 compile

Build Options
-------------
- Export the below environment variables to skip building rocksdb and instead use the system provided rocksdb shared library
- export ERLANG_ROCKSDB_OPTS=-DWITH_SYSTEM_ROCKSDB=ON -DWITH_SNAPPY=ON -DWITH_LZ4=ON

