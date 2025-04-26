# Makefile for LSM Tree Storage

# Variables
JAVA_VERSION = 21
MAVEN = mvn
MAVEN_OPTS = -Dmaven.test.skip=false

# Default target
.PHONY: all
all: clean compile test

# Clean the project
.PHONY: clean
clean:
	$(MAVEN) clean

# Compile the project
.PHONY: compile
compile:
	$(MAVEN) compile

# Run tests
.PHONY: test
test:
	$(MAVEN) test

# Package the project
.PHONY: package
package:
	$(MAVEN) package

# Install the project
.PHONY: install
install:
	$(MAVEN) install

# Run a specific test class
.PHONY: test-class
test-class:
	@if [ -z "$(CLASS)" ]; then \
		echo "Usage: make test-class CLASS=com.umitunal.lsm.LSMStoreTest"; \
	else \
		$(MAVEN) test -Dtest=$(CLASS); \
	fi

# Run a specific test method
.PHONY: test-method
test-method:
	@if [ -z "$(CLASS)" ] || [ -z "$(METHOD)" ]; then \
		echo "Usage: make test-method CLASS=com.umitunal.lsm.LSMStoreTest METHOD=testPutAndGet"; \
	else \
		$(MAVEN) test -Dtest=$(CLASS)#$(METHOD); \
	fi

# Run benchmark tests
.PHONY: test-benchmark
test-benchmark:
	$(MAVEN) test -Dtest="com.umitunal.lsm.benchmark.**"

# Run smoke tests
.PHONY: test-smoke
test-smoke:
	$(MAVEN) test -Dtest="com.umitunal.lsm.smoke.**"

# Run stress tests
.PHONY: test-stress
test-stress:
	$(MAVEN) test -Dtest="com.umitunal.lsm.stress.**"

# Run integration tests
.PHONY: test-it
test-it:
	$(MAVEN) test -Dtest="com.umitunal.lsm.it.**"

# Run all excluded tests
.PHONY: test-all-excluded
test-all-excluded: test-benchmark test-smoke test-stress test-it

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all              - Clean, compile, and test the project"
	@echo "  clean            - Clean the project"
	@echo "  compile          - Compile the project"
	@echo "  test             - Run all tests (excluding benchmark, smoke, stress, and integration tests)"
	@echo "  test-benchmark   - Run benchmark tests"
	@echo "  test-smoke       - Run smoke tests"
	@echo "  test-stress      - Run stress tests"
	@echo "  test-it          - Run integration tests"
	@echo "  test-all-excluded - Run all excluded tests"
	@echo "  package          - Package the project"
	@echo "  install          - Install the project"
	@echo "  test-class       - Run a specific test class (make test-class CLASS=com.umitunal.lsm.LSMStoreTest)"
	@echo "  test-method      - Run a specific test method (make test-method CLASS=com.umitunal.lsm.LSMStoreTest METHOD=testPutAndGet)"
	@echo "  help             - Show this help message"
