# Benchmark of Slonik against other Postgres drivers in Python

This is a very crude benchmark of Slonik against other popular implementations
of Python PostgreSQL drivers.

This purpose is to get a rough idea of where Slonik could be improved
performance wise. This is not an accurate comparison of the other drivers.

## Launching the benchmark

This benchmark requires Python 3.7. Optional dependencies are `psycopg2`,
`asyncpg` and, well… `slonik`.

```
$ python bench.py
--------------------------------------------------------------------------------
Running benchmark columns

With slonik

SELECT 1
Mean: 0.0003 - Median: 0.0003 - Max: 0.0005 - Min: 0.0001
---
SELECT 1, 'foo', 42.21::float
Mean: 0.0002 - Median: 0.0002 - Max: 0.0004 - Min: 0.0002
---
SELECT '3d9d291d-8668-480f-98bf-46ee10d07a5d'::uuid
Mean: 0.0002 - Median: 0.0002 - Max: 0.0003 - Min: 0.0001
---

With psycopg2

SELECT 1
Mean: 0.0001 - Median: 0.0001 - Max: 0.0001 - Min: 0.0001
---
SELECT 1, 'foo', 42.21::float
Mean: 0.0002 - Median: 0.0001 - Max: 0.0006 - Min: 0.0001
---
SELECT '3d9d291d-8668-480f-98bf-46ee10d07a5d'::uuid
Mean: 0.0001 - Median: 0.0001 - Max: 0.0002 - Min: 0.0001
---

With asyncpg

SELECT 1
Mean: 0.0003 - Median: 0.0004 - Max: 0.0006 - Min: 0.0002
---
SELECT 1, 'foo', 42.21::float
Mean: 0.0005 - Median: 0.0003 - Max: 0.0012 - Min: 0.0002
---
SELECT '3d9d291d-8668-480f-98bf-46ee10d07a5d'::uuid
Mean: 0.0003 - Median: 0.0002 - Max: 0.0006 - Min: 0.0002
---
--------------------------------------------------------------------------------
Running benchmark scrolling

With slonik

SELECT generate_series(1, 100)
Mean: 0.0027 - Median: 0.0023 - Max: 0.0045 - Min: 0.0021
---
SELECT generate_series(1, 1000)
Mean: 0.0149 - Median: 0.0150 - Max: 0.0178 - Min: 0.0123
---
SELECT generate_series(1, 10000)
Mean: 0.1303 - Median: 0.1322 - Max: 0.1525 - Min: 0.1106
---

With psycopg2

SELECT generate_series(1, 100)
Mean: 0.0002 - Median: 0.0002 - Max: 0.0004 - Min: 0.0001
---
SELECT generate_series(1, 1000)
Mean: 0.0007 - Median: 0.0006 - Max: 0.0011 - Min: 0.0004
---
SELECT generate_series(1, 10000)
Mean: 0.0044 - Median: 0.0041 - Max: 0.0055 - Min: 0.0038
---

With asyncpg

SELECT generate_series(1, 100)
Mean: 0.0004 - Median: 0.0003 - Max: 0.0010 - Min: 0.0003
---
SELECT generate_series(1, 1000)
Mean: 0.0010 - Median: 0.0009 - Max: 0.0015 - Min: 0.0006
---
SELECT generate_series(1, 10000)
Mean: 0.0050 - Median: 0.0047 - Max: 0.0086 - Min: 0.0039
---
```

Those results reflects the state of Slonik in December 2019. As you can see
it has quite a lot of rooms for improvement compared to other drivers.
