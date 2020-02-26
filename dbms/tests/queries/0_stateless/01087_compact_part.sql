CREATE TABLE s3_test(
    dt Date,
    id UInt64,
    data String,
    INDEX min_max (id) TYPE minmax GRANULARITY 3
) ENGINE=MergeTree()
PARTITION BY dt
ORDER BY (dt, id)
SETTINGS disable_background_merges='true', min_rows_for_wide_part='8192';

INSERT INTO s3_test SELECT '2020-01-03', number, 'test' from system.numbers limit 10000;
SELECT * from s3_test order by dt, id FORMAT Values;

INSERT INTO s3_test SELECT '2020-01-04', number, 'test' from system.numbers limit 10000;
SELECT * from s3_test order by dt, id FORMAT Values;
