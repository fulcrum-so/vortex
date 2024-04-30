<p align="center">
  <a href="https://github.com/spiraldb/vortex">
    <img src=".github/vortex.svg" style="border-radius: 20px" alt="Vortex" />
  </a>
</p>
<p align="center">
    <em>A toolkit for compressed columnar data.</em>
</p>
<p align="center">
<a href="https://github.com/spiraldb/vortex/actions" target="_blank">
    <img src="https://img.shields.io/github/actions/workflow/status/spiraldb/vortex/ci.yml?branch=develop&logo=github&style=" alt="Actions">
</a>
<a href="https://crates.io/crates/vortex-array" target="_blank">
    <img src="https://img.shields.io/crates/v/vortex-array.svg" alt="crates.io">
</a>
<a href="https://docs.rs/vortex-array" target="_blank">
    <img src="https://docs.rs/vortex-array/badge.svg" alt="Documentation">
</a>
<a href="https://github.com/fulcrum-so/vortex" target="_blank">
    <img src="https://img.shields.io/badge/rust-1.76.0%2B-blue.svg?maxAge=3600" alt="Rust">
</a>
<a href="https://github.com/spiraldb/vortex/blob/develop/LICENSE" target="_blank">
    <img src="https://img.shields.io/github/license/spiraldb/vortex" alt="License">
</a>
</p>

---

Vortex is an Apache Arrow-compatible toolkit for working with compressed array data. We are using Vortex to develop a
next-generation columnar file format for multidimensional arrays called Spiral.

> [!CAUTION]
> This library is still under rapid development and is very much a work in progress!
>
> Some key features are not yet implemented, the API will almost certainly change in breaking ways, and we cannot
> yet guarantee correctness in all cases.

The major components of Vortex are (will be!):

* **Logical Types** - a schema definition that makes no assertions about physical layout.
* **Encodings** - a pluggable set of physical layouts. Vortex ships with several state-of-the-art lightweight
  compression codecs that have the potential to support GPU decompression.
* **Compression** - recursive compression based on stratified samples of the input.
* **Compute** - basic compute kernels that can operate over compressed data. Note that Vortex does not intend to become
  a full-fledged compute engine, but rather to provide the ability to implement basic compute operations as may be
  required for efficient scanning & pushdown operations.
* **Statistics** - each array carries around lazily computed summary statistics, optionally populated at read-time.
  These are available to compute kernels as well as to the compressor.
* **Serde** - zero-copy serialization. Useful as a building block in creating IPC or file formats that contain
  compressed arrays.

## Overview: Logical vs Physical

One of the core principles in Vortex is separation of the logical from the physical.

A Vortex array is defined by a logical data type (i.e., the type of scalar elements) as well as a physical encoding
(the type of the array itself). Vortex ships with several built-in encodings, as well as several extension encodings.

The built-in encodings are primarily designed to model the Apache Arrow in-memory format, enabling us to construct
Vortex arrays with zero-copy from Arrow arrays. There are also several built-in encodings (e.g., `sparse` and
`chunked`) that are useful building blocks for other encodings. The included extension encodings are mostly designed
to model compressed in-memory arrays, such as run-length or dictionary encoding.

## Components

### Logical Types

The Vortex type-system is still in flux. The current set of logical types is:

* Null
* Bool
* Integer(8, 16, 32, 64)
* Float(16, b16, 32, 64)
* Binary
* UTF8
* Struct
* Decimal: TODO
* Date/Time/DateTime/Duration: TODO (in-progress, currently partially supported)
* List: TODO
* FixedList: TODO
* Union: TODO

### Canonical/Flat Encodings

Vortex includes a base set of "flat" encodings that are designed to be zero-copy with Apache Arrow. These are the
canonical representations of each of the logical data types. The canonical encodings currently supported are:

* Null
* Bool
* Primitive (Integer, Float)
* Struct
* VarBin
* VarBinView
* ...with more to come

### Compressed Encodings

Vortex includes a set of highly data-parallel, vectorized encodings. These encodings each correspond to a compressed
in-memory array implementation, allowing us to defer decompression. Currently, these are:

* Adaptive Lossless Floating Point (ALP)
* BitPacked (FastLanes)
* Constant
* Chunked
* Delta (FastLanes)
* Dictionary
* Frame-of-Reference
* Run-end Encoding
* RoaringUInt
* RoaringBool
* Sparse
* ZigZag

### Compression

Vortex's top-level compression strategy is based on the
[BtrBlocks](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf) paper.

Roughly, for each chunk of data, a sample of at least ~1% of the data is taken. Compression is then attempted (
recursively) with a set of lightweight encodings. The best-performing combination of encodings is then chosen to encode
the entire chunk. This sounds like it would be very expensive, but given basic statistics about a chunk, it is
possible to cheaply prune many encodings and ensure the search space does not explode in size.

### Compute

Vortex provides the ability for each encoding to specialize the implementation of a compute function to avoid
decompressing where possible. For example, filtering a dictionary-encoded UTF8 array can be more cheaply performed by
filtering the dictionary first.

Note that Vortex does not intend to become a full-fledged compute engine, but rather to provide the ability to
implement basic compute operations as may be required for efficient scanning & operation pushdown.

### Statistics

Vortex arrays carry lazily-computed summary statistics. Unlike other array libraries, these statistics can be populated
from disk formats such as Parquet and preserved all the way into a compute engine. Statistics are available to compute
kernels as well as to the compressor.

The current statistics are:

* BitWidthFreq
* TrailingZeroFreq
* IsConstant
* IsSorted
* IsStrictSorted
* Max
* Min
* RunCount
* TrueCount
* NullCount

### Serialization / Deserialization (Serde)

Vortex serde is currently in the design phase. The goals of this implementation are:

* Support scanning (column projection + row filter) with zero-copy and zero heap allocation.
* Support random access in constant time.
* Forward statistical information (such as sortedness) to consumers.
* To provide a building block for file format authors to store compressed array data.

## Integration with Apache Arrow

Apache Arrow is the de facto standard for interoperating on columnar array data. Naturally, Vortex is designed to
be maximally compatible with Apache Arrow. All Arrow arrays can be converted into Vortex arrays with zero-copy,
and a Vortex array constructed from an Arrow array can be converted back to Arrow, again with zero-copy.

It is important to note that Vortex and Arrow have different--albeit complementary--goals.

Vortex explicitly separates logical types from physical encodings, distinguishing it from Arrow. This allows
Vortex to model more complex arrays while still exposing a logical interface. For example, Vortex can model a UTF8
`ChunkedArray` where the first chunk is run-length encoded and the second chunk is dictionary encoded.
In Arrow, `RunLengthArray` and `DictionaryArray` are separate incompatible types, and so cannot be combined in this way.

## Contributing

While we hope to turn Vortex into a community project, its current rapid rate of change makes taking contributions
without prior discussion infeasible. If you are interested in contributing, please open an issue to discuss your ideas.

## Setup

This repo uses submodules for non-Rust dependencies (e.g., for the zig fastlanez repo). Before building make sure to run

```bash
git submodule update --init --recursive

# Install the zig version required by fastlanez
./zigup

# Install Rye from https://rye-up.com, and setup the virtualenv
rye sync
```

## License

Licensed under the Apache License, Version 2.0 (the "License").

## Acknowledgments 🏆

This project is inspired by and--in some cases--directly based upon the existing, excellent work of many researchers
and OSS developers.

In particular, the following academic papers greatly influenced the development:

* Maximilian Kuschewski, David Sauerwein, Adnan Alhomssi, and Viktor Leis.
    2023. [BtrBlocks: Efficient Columnar Compression
          for Data Lakes](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf). Proc. ACM Manag. Data 1,
          2,
          Article 118 (June 2023), 14 pages. https://doi.org/10.1145/3589263
* Azim Afroozeh and Peter
  Boncz. [The FastLanes Compression Layout: Decoding >100 Billion Integers per Second with Scalar
  Code](https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf). PVLDB, 16(9): 2132 - 2144, 2023.
* Peter Boncz, Thomas Neumann, and Viktor Leis. [FSST: Fast Random Access String
  Compression](https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf).
  PVLDB, 13(11): 2649-2661, 2020.
* Azim Afroozeh, Leonardo X. Kuffo, and Peter Boncz. 2023. [ALP: Adaptive Lossless floating-Point
  Compression](https://ir.cwi.nl/pub/33334/33334.pdf). Proc. ACM
  Manag. Data 1, 4 (SIGMOD), Article 230 (December 2023), 26 pages. https://doi.org/10.1145/3626717

Additionally, we benefited greatly from:

* the collected OSS work of [Daniel Lemire](https://github.com/lemire), such
  as [FastPFor](https://github.com/lemire/FastPFor),
  and [StreamVByte](https://github.com/lemire/streamvbyte).
* the [parquet2](https://github.com/jorgecarleitao/parquet2) project
  by [Jorge Leitao](https://github.com/jorgecarleitao).
* the public discussions around choices of compression codecs, as well as the C++ implementations thereof,
  from [duckdb](https://github.com/duckdb/duckdb).
* the existence, ideas, & implementation of the [Apache Arrow](https://arrow.apache.org) project.
* the [Velox](https://github.com/facebookincubator/velox) project and discussions with its maintainers.

Thanks to all of the aforementioned for sharing their work and knowledge with the world! 🚀
