# --8<-- [start:setup]
from pyspark.sql import DataFrame
import sys
from typing import Any

# --8<-- [end:setup]


# --8<-- [start:column_to_list]


def column_to_list(df: DataFrame, col_name: str) -> list[Any]:
    pyarrow_kv = ("spark.sql.execution.arrow.pyspark.enabled", "true")

    if "pyspark" not in sys.modules:
        raise ImportError

    # sparksession from df is not available in older versions of pyspark
    if sys.modules["pyspark"].__version__ < "3.3.0":
        return df.select(col_name).rdd.flatMap(lambda x: x).collect()

    spark_config = df.sparkSession.sparkContext.getConf().getAll()

    pyarrow_enabled: bool = pyarrow_kv in spark_config
    pyarrow_valid = pyarrow_enabled and sys.modules["pyarrow"] >= "0.17.0"

    pandas_exists = "pandas" in sys.modules
    pandas_valid = pandas_exists and sys.modules["pandas"].__version__ >= "0.24.2"

    if pyarrow_valid and pandas_valid:
        return df.select(col_name).toPandas()[col_name].tolist()

    return df.select(col_name).rdd.flatMap(lambda x: x).collect()


# --8<-- [end:column_to_list]
