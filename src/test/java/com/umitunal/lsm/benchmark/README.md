# LSMStore Benchmarks

This package contains JMH (Java Microbenchmark Harness) benchmarks for the LSMStore implementation.

## Overview

The benchmarks measure the performance of various operations provided by the LSMStore:

- **Put Operation**: Measures the time taken to store a key-value pair
- **Get Operation**: Measures the time taken to retrieve a value by key
- **Delete Operation**: Measures the time taken to remove a key-value pair
- **Range Query**: Measures the time taken to retrieve key-value pairs within a specified range
- **Iterator**: Measures the time taken to iterate over keys in a range
- **Sequential Put**: Measures the time taken to perform multiple sequential put operations
- **Put with TTL**: Measures the time taken to store a key-value pair with a time-to-live value

## Running the Benchmarks

### From Maven

To run the benchmarks using Maven:

```bash
mvn clean package
java -jar target/benchmarks.jar
```

### From IDE

You can also run the benchmarks directly from your IDE by executing the `main` method in the `LSMStoreBenchmark` class.

## Benchmark Configuration

The benchmarks are configured with the following parameters:

- **Measurement Mode**: Average time per operation
- **Output Time Unit**: Microseconds
- **Warmup**: 3 iterations, 1 second each
- **Measurement**: 5 iterations, 1 second each
- **Fork**: 1 JVM instance with 2GB heap size
- **Data Size**: 10,000 key-value pairs
- **Key Size**: 16 bytes
- **Value Size**: 100 bytes

## Interpreting Results

The benchmark results will show the average time taken for each operation in microseconds. Lower values indicate better performance.

Example output:

```
Benchmark                                Mode  Cnt    Score    Error  Units
LSMStoreBenchmark.benchmarkDelete        avgt    5  123.456 ±  1.234  us/op
LSMStoreBenchmark.benchmarkGet           avgt    5   45.678 ±  0.456  us/op
LSMStoreBenchmark.benchmarkGetIterator   avgt    5  789.012 ±  7.890  us/op
LSMStoreBenchmark.benchmarkGetRange      avgt    5  567.890 ±  5.678  us/op
LSMStoreBenchmark.benchmarkPut           avgt    5  234.567 ±  2.345  us/op
LSMStoreBenchmark.benchmarkPutWithTTL    avgt    5  245.678 ±  2.456  us/op
LSMStoreBenchmark.benchmarkSequentialPut avgt    5  890.123 ±  8.901  us/op
```

## Customizing Benchmarks

You can customize the benchmarks by modifying the following parameters in the `LSMStoreBenchmark` class:

- `DATA_SIZE`: Number of key-value pairs to generate
- `KEY_SIZE`: Size of each key in bytes
- `VALUE_SIZE`: Size of each value in bytes

You can also modify the JMH annotations to change the benchmark configuration:

- `@BenchmarkMode`: Measurement mode (e.g., Throughput, AverageTime)
- `@OutputTimeUnit`: Time unit for results (e.g., MILLISECONDS, MICROSECONDS)
- `@Warmup`: Number and duration of warmup iterations
- `@Measurement`: Number and duration of measurement iterations
- `@Fork`: Number of JVM instances and JVM arguments