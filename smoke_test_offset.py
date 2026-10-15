import sys
sys.path.insert(0, "/Users/ramswaroopdevakumar/opensearch-benchmark")

from osbenchmark.workload.params import BulkVectorsFromDataSetParamSource

DATA_PATH = "/Users/ramswaroopdevakumar/osb-data/sift-128-euclidean.hdf5"


def make_source(num_vectors=10, bulk_size=5, offset=None):
    params = {
        "field": "target_field",
        "data_set_format": "hdf5",
        "data_set_path": DATA_PATH,
        "num_vectors": num_vectors,
        "bulk_size": bulk_size,
        "index": "smoke_test_index",
    }
    if offset is not None:
        params["offset"] = offset
    return BulkVectorsFromDataSetParamSource(workload=None, params=params)


def ids_from(partition, n_batches):
    ids = []
    for _ in range(n_batches):
        result = partition.params()
        body = result["body"]
        ids.extend(meta["index"]["_id"] for meta in body[0::2])
    return ids


print("=== Backward-compat check: no offset param at all ===")
src0 = make_source(num_vectors=10, bulk_size=5)
p0 = src0.partition(0, 1)
print(f"partition.offset = {p0.offset} (expect 0)")
print(f"doc _ids = {ids_from(p0, 2)} (expect [0..9])")

print()
print("=== Single client, offset=100 ===")
src1 = make_source(num_vectors=10, bulk_size=5, offset=100)
p1 = src1.partition(0, 1)
print(f"partition.offset = {p1.offset} (expect 100)")
print(f"partition.current = {p1.current} (expect 100)")
print(f"doc _ids = {ids_from(p1, 2)} (expect [100..109])")

print()
print("=== Two concurrent clients, offset=1000, num_vectors=10 ===")
src2 = make_source(num_vectors=10, bulk_size=5, offset=1000)
c0 = src2.partition(0, 2)
c1 = src2.partition(1, 2)
print(f"client0.offset = {c0.offset} (expect 1000)")
print(f"client1.offset = {c1.offset} (expect 1005)")
print(f"client0 _ids = {ids_from(c0, 1)} (expect [1000..1004])")
print(f"client1 _ids = {ids_from(c1, 1)} (expect [1005..1009])")
