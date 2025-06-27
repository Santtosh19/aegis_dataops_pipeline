# tests/test_pipeline.py
import pytest
from hypothesis import given
from hypothesis import strategies as st
from pyspark.sql import SparkSession

from aegis.pipeline import safe_cast_to_double, validate_and_separate_data


@pytest.fixture(scope="session")
def spark():
    """Pytest fixture to create a shared SparkSession for all tests."""
    return SparkSession.builder.master("local[2]").appName("AegisTests").getOrCreate()


def test_safe_cast_udf():
    """Tests our UDF for handling valid, invalid, and null inputs."""
    assert safe_cast_to_double("123.45") == 123.45
    assert safe_cast_to_double("INVALID") is None
    assert safe_cast_to_double(None) is None


def test_validation_and_separation(spark):
    """Tests our main data quality gateway function."""
    # Create some sample data
    schema = ["device_id", "status_code", "temperature"]
    data = [
        ("dev_1", "200", "75.2"),  # Good record
        (None, "200", "76.1"),  # Bad record (null id)
        ("dev_3", "999", "77.3"),  # Bad record (invalid status)
        ("dev_4", "200", "INVALID"),  # Bad record (invalid temp)
        ("dev_5", "500", "80.0"),  # Good record
    ]
    df = spark.createDataFrame(data, schema)

    # Run the function
    clean_df, quarantined_df = validate_and_separate_data(df)

    # Assert the results
    assert clean_df.count() == 2
    assert quarantined_df.count() == 3

    # Check that the correct devices are in the clean df
    clean_devices = [row.device_id for row in clean_df.collect()]
    assert "dev_1" in clean_devices
    assert "dev_5" in clean_devices


@given(st.one_of(st.floats(allow_nan=False, allow_infinity=False), st.text()))
def test_safe_cast_udf_hypothesis(value):
    """
    Property-based test for the casting UDF.
    Hypothesis will generate hundreds of different float and text examples to find edge cases.
    """
    try:
        # The UDF's Python function expects a single value, not a Spark column
        result = safe_cast_to_double(str(value))
        # The property we are testing is: the result must ALWAYS be
        # either a float or None. The function should never crash.
        assert isinstance(result, (float, type(None)))
    except Exception as e:
        # If any other exception occurs, the test fails.
        pytest.fail(f"UDF failed on input '{value}' with exception: {e}")
