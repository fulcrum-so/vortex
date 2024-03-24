import os.path
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import vortex


def test_primitive_compress():
    a = pa.array([0, 0, 0, 0, 9, 9, 9, 9, 1, 5])
    arr_compressed = vortex.compress(vortex.encode(a))
    assert not isinstance(arr_compressed, vortex.PrimitiveArray)
    assert arr_compressed.nbytes < a.nbytes


def test_for_compress():
    a = pa.array(np.arange(10_000) + 10_000_000)
    arr_compressed = vortex.compress(vortex.encode(a))
    assert not isinstance(arr_compressed, vortex.PrimitiveArray)


def test_bool_compress():
    a = vortex.encode(pa.array([False] * 10_000 + [True] * 10_000))
    arr_compressed = vortex.compress(a)
    assert len(arr_compressed) == 20_000
    assert isinstance(arr_compressed, vortex.RoaringBoolArray)
    assert arr_compressed.nbytes < a.nbytes


def test_roaring_bool_encode():
    a = vortex.encode(pa.array([True] * 10_000))
    rarr = vortex.RoaringBoolArray.encode(a)
    assert isinstance(rarr, vortex.RoaringBoolArray)
    assert rarr.nbytes < a.nbytes


def test_arange_encode():
    a = vortex.encode(pa.array(np.arange(10_000), type=pa.uint32()))
    compressed = vortex.compress(a)
    assert isinstance(compressed, vortex.DeltaArray) or isinstance(compressed, vortex.RoaringIntArray)
    assert compressed.nbytes < a.nbytes


def test_zigzag_encode():
    a = vortex.encode(pa.array([-1, -1, 0, -1, 1, -1]))
    zarr = vortex.ZigZagArray.encode(a)
    assert isinstance(zarr, vortex.ZigZagArray)
    # TODO(ngates): support decoding once we have decompressor.


def test_chunked_encode():
    chunked = pa.chunked_array([pa.array([0, 1, 2]), pa.array([3, 4, 5])])
    encoded = vortex.encode(chunked)
    assert isinstance(encoded, vortex.ChunkedArray)
    assert encoded.to_pyarrow().combine_chunks() == pa.array([0, 1, 2, 3, 4, 5])


def test_table_encode():
    table = pa.table(
        {
            "number": pa.chunked_array([pa.array([0, 1, 2]), pa.array([3, 4, 5])]),
            "string": pa.chunked_array([pa.array(["a", "b", "c"]), pa.array(["d", "e", "f"])]),
        }
    )
    encoded = vortex.encode(table)
    assert isinstance(encoded, vortex.ChunkedArray)
    assert encoded.to_pyarrow().combine_chunks() == pa.StructArray.from_arrays(
        [pa.array([0, 1, 2, 3, 4, 5]), pa.array(["a", "b", "c", "d", "e", "f"])], names=["number", "string"]
    )


@pytest.mark.xfail(reason="Not yet implemented")
def test_taxi():
    curdir = Path(os.path.dirname(__file__)).parent.parent
    table = pq.read_table(curdir / "bench-vortex/data/yellow-tripdata-2023-11.parquet")
    compressed = vortex.compress(vortex.encode(table[:100]))
    decompressed = compressed.to_pyarrow()
    assert not decompressed
