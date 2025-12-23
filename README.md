PCH Query 5 – C++ Multithreaded Implementation
Overview

This project implements TPC-H Query 5 in C++ with support for multithreaded execution.
The goal is to evaluate query performance using single-threaded and multi-threaded (4 threads) execution on TPC-H Scale Factor 2 (SF2) data.

The implementation reads raw .tbl files generated using the TPC-H DBGEN tool, performs the required joins and aggregations in memory, and outputs the final result sorted by revenue.# TPCH Query 5 C++ Multithreading Project


 Query 5(Reference)
 SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC;

* Features

Implements full TPCH Query 5 logic

Supports configurable thread count

Uses C++ standard threading (std::thread)

Reads TPCH .tbl files directly

Outputs results sorted by revenue

Tested on Scale Factor 2 (SF2) dataset


*Prerequisites

CMake ≥ 3.10

C++ compiler (C++11 or later)
(tested with Apple Clang)

TPC-H DBGEN tool (for data generation)


*TPCH Data Generation (SF2)

git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make
./dbgen -s 2
mkdir -p ~/tpch_data_sf2
mv *.tbl ~/tpch_data_sf2/

Expected tables:
customer.tbl
orders.tbl
lineitem.tbl
supplier.tbl
nation.tbl
region.tbl

*Building the Project
git clone https://github.com/sahuruchi21/tpch-query5.git
cd tpch-query5
mkdir build
cd build
cmake ..
make
After compilation, the executable tpch_query5 will be generated.

*Running the Program

Single-Threaded Execution
./tpch_query5 \
--r_name ASIA \
--start_date 1994-01-01 \
--end_date 1995-01-01 \
--threads 1 \
--table_path ~/tpch_data_sf2 \
--result_path ~/tpch_data_sf2/results/result_single.txt

Multi-Threaded Execution (4 Threads)
./tpch_query5 \
--r_name ASIA \
--start_date 1994-01-01 \
--end_date 1995-01-01 \
--threads 4 \
--table_path ~/tpch_data_sf2 \
--result_path ~/tpch_data_sf2/results/result_4threads.txt

Final Result (SF2)

Output file format:
n_name|revenue
File location:

~/tpch_data_sf2/results/result_single.txt
(The result is sorted in descending order of revenue.)


Runtime Results (SF2)

| Execution Mode  | Threads | Time Taken   |
| --------------- | ------- | ------------ |
| Single-threaded | 1       | ~110 seconds |
| Multi-threaded  | 4       | ~96 seconds  |


*Speedup Analysis

Parallelization: Lineitem processing is divided across multiple threads.

Improved CPU utilization: Multithreading increases CPU usage from ~94% to ~102%.

Limited speedup is expected due to:

Large memory access

Join-heavy workload

Thread synchronization overhead


*Project Structure

tpch-query5/
├── src/
│   ├── main.cpp
│   ├── query5.cpp
│   └── query5.hpp
├── CMakeLists.txt
├── README.md
└── build/


*Submission Details

GitHub Repository:
https://github.com/sahuruchi21/tpch-query5

Scale Factor: SF2

Result File: result_single.txt

*Notes

Ensure TPCH .tbl files are generated correctly.

The result directory must exist before execution.

Thread count can be adjusted based on system capability.

*Author

Ruchi Sahu
