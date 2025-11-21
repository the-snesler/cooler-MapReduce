# MapReduce Testing Suite

Comprehensive test suite for the coolest-mapreduce system.

## Test Structure

```
tests/
├── unit/                    # Unit tests (no external dependencies)
│   ├── test_map_executor.py       # Map task execution tests
│   ├── test_reduce_executor.py    # Reduce task execution tests
│   └── test_function_loader.py    # Function loading tests
├── integration/             # Integration tests (require Docker)
│   ├── test_end_to_end.py        # Complete job execution tests
│   └── test_correctness.py       # Output correctness validation
├── conftest.py             # Shared pytest fixtures
└── README.md               # This file
```

## Running Tests

### Quick Start

Run all tests:
```bash
./run_tests.sh
```

### Unit Tests Only

Run unit tests without Docker:
```bash
pytest tests/unit/ -v
```

### Integration Tests Only

Run integration tests (requires Docker):
```bash
docker-compose up -d
pytest tests/integration/ -v -m integration
```

### Specific Test File

Run tests from a specific file:
```bash
pytest tests/unit/test_map_executor.py -v
```

### Specific Test Class or Function

Run a specific test:
```bash
pytest tests/unit/test_map_executor.py::TestMapExecutorPartitioning::test_same_key_goes_to_same_partition -v
```

## Test Categories

### Unit Tests

- **No external dependencies** - Can run without Docker or services
- **Fast execution** - Complete in seconds
- **Mock external dependencies** - Use mocks for file I/O and modules
- **High coverage goal** - Target >90% code coverage

Unit test categories:
- Map executor: input splitting, partitioning, combiner application
- Reduce executor: key grouping, output sorting, aggregation
- Function loader: dynamic module loading, error handling

### Integration Tests

- **Require Docker** - Need running coordinator and worker containers
- **Slower execution** - May take minutes to complete
- **Test real system** - Use actual Docker containers and gRPC communication
- **Marked with `@pytest.mark.integration`**

Integration test categories:
- End-to-end job execution
- Performance metrics collection
- Output correctness validation

## Test Fixtures

Common fixtures defined in `conftest.py`:

- `temp_dir` - Temporary directory for test files (auto-cleaned)
- `sample_text` - Sample text data for testing
- `sample_input_file` - Pre-created input file with sample text
- `wordcount_job_file` - Path to word count example
- `inverted_index_job_file` - Path to inverted index example

## Coverage Reports

Generate coverage reports:

```bash
# Terminal report
pytest tests/unit/ --cov=worker --cov=coordinator --cov-report=term-missing

# HTML report
pytest tests/unit/ --cov=worker --cov=coordinator --cov-report=html
open htmlcov/index.html
```

## Writing New Tests

### Unit Test Example

```python
def test_my_feature(temp_dir, sample_input_file):
    """Test description"""
    # Arrange
    executor = MapExecutor(...)

    # Act
    result = executor.execute()

    # Assert
    assert result['success'] is True
```

### Integration Test Example

```python
@pytest.mark.integration
def test_my_integration():
    """Integration test requiring Docker"""
    # Check Docker is running
    result = subprocess.run(['docker-compose', 'ps'], ...)
    if 'coordinator' not in result.stdout:
        pytest.skip("Docker not running")

    # Test logic...
```

## Troubleshooting

### Tests Fail with Import Errors

Install test dependencies:
```bash
pip install -r requirements-test.txt
```

### Integration Tests Skip or Fail

1. Ensure Docker is running:
   ```bash
   docker-compose ps
   ```

2. Start containers if needed:
   ```bash
   docker-compose up -d
   ```

3. Check container logs:
   ```bash
   docker-compose logs coordinator
   docker-compose logs worker
   ```

### Permission Errors on Test Files

Ensure test runner is executable:
```bash
chmod +x run_tests.sh
```

### Slow Test Execution

Run only fast tests:
```bash
pytest tests/unit/ -v -m "not slow"
```

## CI/CD Integration

Tests are designed to run in CI/CD pipelines:

1. Unit tests run on every commit
2. Integration tests run on pull requests
3. Coverage reports uploaded automatically

See `.github/workflows/tests.yml` for CI configuration.

## Best Practices

1. **Keep tests independent** - Each test should work in isolation
2. **Use fixtures** - Reuse common setup via pytest fixtures
3. **Clear test names** - Name tests to describe what they verify
4. **Fast unit tests** - Keep unit tests under 1 second each
5. **Mock external deps** - Unit tests should not hit real services
6. **Clean up resources** - Use fixtures or try/finally for cleanup

## Test Markers

- `@pytest.mark.integration` - Requires Docker
- `@pytest.mark.slow` - Takes longer than 5 seconds
- `@pytest.mark.skip` - Temporarily disabled
- `@pytest.mark.parametrize` - Run test with multiple inputs
