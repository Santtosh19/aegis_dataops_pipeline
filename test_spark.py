# test_spark.py
import sys

import pyspark
from pyspark.sql import SparkSession

print("--- Environment Verification ---")
print(f"Python Executable: {sys.executable}")
print(f"PySpark Library Location: {pyspark.__file__}")
print(f"PySpark Version: {pyspark.__version__}")
print("----------------------------\n")

try:
    from pyspark.sql.functions import try_cast

    print("SUCCESS: 'try_cast' was imported successfully!")
except ImportError as e:
    print(f"FAILURE: Could not import 'try_cast'. Error: {e}")
    # Exit so we don't bother starting Spark
    sys.exit(1)


print("\nAttempting to start Spark and use try_cast...")
spark = SparkSession.builder.appName("TestTryCast").master("local[1]").getOrCreate()

data = [("1",), ("2",), ("three",)]
df = spark.createDataFrame(data, ["value_str"])

df_cast = df.withColumn("value_int", try_cast(df.value_str, "integer"))

print("DataFrame with try_cast result:")
df_cast.show()

spark.stop()
print("\nTest completed successfully.")
