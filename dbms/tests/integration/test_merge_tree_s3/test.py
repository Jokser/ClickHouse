import logging
import random
import string

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(cluster):
    minio_client = cluster.minio_client

    if minio_client.bucket_exists(cluster.minio_bucket):
        minio_client.remove_bucket(cluster.minio_bucket)

    minio_client.make_bucket(cluster.minio_bucket)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node", config_dir="configs", with_minio=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        yield cluster
    finally:
        cluster.shutdown()


def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def generate_values(date_str, count):
    data = [[date_str, random.randint(0, 1000000000), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def test_log_family_s3(cluster):
    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("""
        CREATE TABLE s3_test(
            dt Date,
            id UInt64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=MergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS disable_background_merges='true', min_rows_for_wide_part='8192' 
    """)

    values1 = generate_values('2020-01-03', 10000)
    node.query("INSERT INTO s3_test VALUES {}".format(values1))
    assert node.query("SELECT * FROM s3_test FORMAT Values") == values1
    #assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == files_overhead_per_insert + files_overhead

    #node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-05', 1)))

    values2 = generate_values('2020-01-04', 10000)
    node.query("INSERT INTO s3_test VALUES {}".format(values2))
    logging.info(node.query("SELECT * FROM s3_test ORDER BY dt, id FORMAT Values"))
    assert node.query("SELECT * FROM s3_test ORDER BY dt, id FORMAT Values") == values1 + "," + values2

    #assert node.query("SELECT * FROM s3_test where id = 4 FORMAT Values") == "(4,'data2')"

    #node.query("INSERT INTO s3_test SELECT number + 5 FROM numbers(3)")
    #assert node.query("SELECT * FROM s3_test order by id") == "0\n1\n2\n3\n4\n5\n6\n7\n"
    #assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == files_overhead_per_insert * 2 + files_overhead

    #node.query("INSERT INTO s3_test SELECT number + 8 FROM numbers(1)")
    #assert node.query("SELECT * FROM s3_test order by id") == "0\n1\n2\n3\n4\n5\n6\n7\n8\n"
    #assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == files_overhead_per_insert * 3 + files_overhead

    #node.query("TRUNCATE TABLE s3_test")
    #assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == 0

    #node.query("DROP TABLE s3_test")
