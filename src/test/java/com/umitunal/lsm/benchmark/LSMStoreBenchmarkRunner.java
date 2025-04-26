/*
 * Copyright (c) 2023-2025 Umit Unal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.umitunal.lsm.benchmark;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * Runner class for LSMStore benchmarks.
 * 
 * This class provides a convenient way to run the LSMStore benchmarks with
 * customizable options. It can be executed directly from an IDE or from the
 * command line.
 */
public class LSMStoreBenchmarkRunner {

    /**
     * JUnit test method to run the benchmarks.
     * This allows the benchmarks to be run using Maven's test runner.
     * Uses a quick benchmark configuration to reduce test execution time.
     * 
     * @throws RunnerException If an error occurs during benchmark execution
     */
    @Test
    public void runBenchmarks() throws RunnerException {
        System.out.println("Starting LSMStore benchmarks...");
        runQuickBenchmark();
        System.out.println("LSMStore benchmarks completed.");
    }

    /**
     * Main method to run the benchmarks.
     * 
     * @param args Command line arguments (not used)
     * @throws RunnerException If an error occurs during benchmark execution
     */
    public static void main(String[] args) throws RunnerException {
        runDefaultBenchmarks();
    }

    /**
     * Run benchmarks with default options.
     * 
     * @throws RunnerException If an error occurs during benchmark execution
     */
    private static void runDefaultBenchmarks() throws RunnerException {
        // Build options for the benchmark runner
        Options options = new OptionsBuilder()
            // Include only LSMStoreBenchmark class
            .include(LSMStoreBenchmark.class.getSimpleName())
            // Warm up for 3 iterations, 1 second each
            .warmupIterations(3)
            .warmupTime(TimeValue.seconds(1))
            // Measure for 5 iterations, 1 second each
            .measurementIterations(5)
            .measurementTime(TimeValue.seconds(1))
            // Fork 1 JVM with 2GB heap
            .forks(1)
            .jvmArgs("-Xms2G", "-Xmx2G", "--enable-preview")
            // Output results to console
            .shouldDoGC(true)
            .shouldFailOnError(true)
            .resultFormat(ResultFormatType.TEXT)
            .result("lsm-store-benchmark-results.txt")
            .timeUnit(TimeUnit.MICROSECONDS)
            .build();

        // Run the benchmark
        new Runner(options).run();

        System.out.println("Benchmark completed. Results saved to lsm-store-benchmark-results.txt");
    }

    /**
     * Run the benchmarks with custom options.
     * 
     * @param warmupIterations Number of warmup iterations
     * @param measurementIterations Number of measurement iterations
     * @param forks Number of JVM forks
     * @param resultFile File to save results to
     * @throws RunnerException If an error occurs during benchmark execution
     */
    public static void runBenchmarks(int warmupIterations, int measurementIterations, 
                                    int forks, String resultFile) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(LSMStoreBenchmark.class.getSimpleName())
            .warmupIterations(warmupIterations)
            .warmupTime(TimeValue.seconds(1))
            .measurementIterations(measurementIterations)
            .measurementTime(TimeValue.seconds(1))
            .forks(forks)
            .jvmArgs("-Xms2G", "-Xmx2G", "--enable-preview")
            .shouldDoGC(true)
            .shouldFailOnError(true)
            .resultFormat(ResultFormatType.TEXT)
            .result(resultFile)
            .timeUnit(TimeUnit.MICROSECONDS)
            .build();

        new Runner(options).run();

        System.out.println("Benchmark completed. Results saved to " + resultFile);
    }

    /**
     * Run a quick benchmark with minimal iterations.
     * This is useful for testing the benchmark setup without running a full benchmark.
     * 
     * @throws RunnerException If an error occurs during benchmark execution
     */
    public static void runQuickBenchmark() throws RunnerException {
        System.out.println("Running quick benchmark...");
        Options options = new OptionsBuilder()
            .include(LSMStoreBenchmark.class.getSimpleName())
            .warmupIterations(1)
            .warmupTime(TimeValue.seconds(1))
            .measurementIterations(1)
            .measurementTime(TimeValue.seconds(1))
            .forks(1)
            .jvmArgs("-Xms1G", "-Xmx1G", "--enable-preview")
            .shouldDoGC(true)
            .shouldFailOnError(true)
            .resultFormat(ResultFormatType.TEXT)
            .result("lsm-store-quick-benchmark-results.txt")
            .timeUnit(TimeUnit.MICROSECONDS)
            .build();

        new Runner(options).run();
        System.out.println("Quick benchmark completed. Results saved to lsm-store-quick-benchmark-results.txt");
    }
}
