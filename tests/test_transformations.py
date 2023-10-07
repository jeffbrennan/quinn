import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
)

import quinn
from pyspark.sql import DataFrame
from tests.conftest import auto_inject_fixtures
import chispa
import chispa.schema_comparer


@auto_inject_fixtures("spark")
def describe_with_columns_renamed():
    def it_renames_spaces_to_underscores(spark):
        def spaces_to_underscores(s):
            return s.replace(" ", "_")

        schema = StructType(
            [
                StructField("i like cheese", StringType(), True),
                StructField("yummy stuff", StringType(), True),
            ]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.with_columns_renamed(spaces_to_underscores)(source_df)
        expected_df = quinn.create_df(
            spark,
            [("jose", "a"), ("li", "b"), ("sam", "c")],
            [
                ("i_like_cheese", StringType(), True),
                ("yummy_stuff", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)

    def it_renames_dots_to_underscores(spark):
        def dots_to_underscores(s):
            return s.replace(".", "_")

        schema = StructType(
            [
                StructField("i.like.cheese", StringType(), True),
                StructField("yummy.stuff", StringType(), True),
            ]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.with_columns_renamed(dots_to_underscores)(source_df)
        expected_df = quinn.create_df(
            spark,
            [("jose", "a"), ("li", "b"), ("sam", "c")],
            [
                ("i_like_cheese", StringType(), True),
                ("yummy_stuff", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)


def describe_with_some_columns_renamed():
    def it_renames_columns_based_on_a_map(spark):
        mapping = {"chips": "french_fries", "petrol": "gas"}

        def british_to_american(s):
            return mapping[s]

        def change_col_name(s):
            return s in mapping

        schema = StructType(
            [
                StructField("chips", StringType(), True),
                StructField("hi", StringType(), True),
                StructField("petrol", StringType(), True),
            ]
        )
        data = [("potato", "hola!", "disel")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.with_some_columns_renamed(
            british_to_american, change_col_name
        )(source_df)
        expected_df = quinn.create_df(
            spark,
            [("potato", "hola!", "disel")],
            [
                ("french_fries", StringType(), True),
                ("hi", StringType(), True),
                ("gas", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)

    def it_renames_some_columns_with_dots(spark):
        def dots_to_underscores(s):
            return s.replace(".", "_")

        def change_col_name(s):
            return s.startswith("a")

        schema = StructType(
            [
                StructField("a.person", StringType(), True),
                StructField("a.thing", StringType(), True),
                StructField("b.person", StringType(), True),
            ]
        )
        data = [("frank", "hot dog", "mia")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.with_some_columns_renamed(
            dots_to_underscores, change_col_name
        )(source_df)
        expected_df = quinn.create_df(
            spark,
            [("frank", "hot dog", "mia")],
            [
                ("a_person", StringType(), True),
                ("a_thing", StringType(), True),
                ("b.person", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)


def describe_snake_case_col_names():
    def it_snake_cases_col_names(spark):
        schema = StructType(
            [
                StructField("I like CHEESE", StringType(), True),
                StructField("YUMMMMY stuff", StringType(), True),
            ]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.snake_case_col_names(source_df)
        expected_df = quinn.create_df(
            spark,
            [("jose", "a"), ("li", "b"), ("sam", "c")],
            [
                ("i_like_cheese", StringType(), True),
                ("yummmmy_stuff", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)


def describe_sort_columns():
    def it_sorts_columns_in_asc_order(spark):
        source_df = quinn.create_df(
            spark,
            [
                ("jose", "oak", "switch"),
                ("li", "redwood", "xbox"),
                ("luisa", "maple", "ps4"),
            ],
            [
                ("name", StringType(), True),
                ("tree", StringType(), True),
                ("gaming_system", StringType(), True),
            ],
        )
        actual_df = quinn.sort_columns(source_df, "asc")
        expected_df = quinn.create_df(
            spark,
            [
                ("switch", "jose", "oak"),
                ("xbox", "li", "redwood"),
                ("ps4", "luisa", "maple"),
            ],
            [
                ("gaming_system", StringType(), True),
                ("name", StringType(), True),
                ("tree", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)

    def it_sorts_columns_in_desc_order(spark):
        source_df = quinn.create_df(
            spark,
            [
                ("jose", "oak", "switch"),
                ("li", "redwood", "xbox"),
                ("luisa", "maple", "ps4"),
            ],
            [
                ("name", StringType(), True),
                ("tree", StringType(), True),
                ("gaming_system", StringType(), True),
            ],
        )
        actual_df = quinn.sort_columns(source_df, "desc")
        expected_df = quinn.create_df(
            spark,
            [
                ("oak", "jose", "switch"),
                ("redwood", "li", "xbox"),
                ("maple", "luisa", "ps4"),
            ],
            [
                ("tree", StringType(), True),
                ("name", StringType(), True),
                ("gaming_system", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)

    def it_throws_an_error_if_the_sort_order_is_invalid(spark):
        source_df = quinn.create_df(
            spark,
            [
                ("jose", "oak", "switch"),
                ("li", "redwood", "xbox"),
                ("luisa", "maple", "ps4"),
            ],
            [
                ("name", StringType(), True),
                ("tree", StringType(), True),
                ("gaming_system", StringType(), True),
            ],
        )
        with pytest.raises(ValueError) as excinfo:
            quinn.sort_columns(source_df, "cats")
        assert (
            excinfo.value.args[0]
            == "['asc', 'desc'] are the only valid sort orders and you entered a sort order of 'cats'"
        )


def _test_sort_struct_flat(spark, sort_order: str):
    def _get_simple_test_dataframes(sort_order) -> tuple[(DataFrame, DataFrame)]:
        col_a = 1
        col_b = 2
        col_c = 3

        unsorted_fields = StructType(
            [
                StructField("b", IntegerType()),
                StructField("c", IntegerType()),
                StructField("a", IntegerType()),
            ]
        )
        unsorted_data = [
            (col_b, col_c, col_a),
        ]
        if sort_order == "asc":
            sorted_fields = StructType(
                [
                    StructField("a", IntegerType()),
                    StructField("b", IntegerType()),
                    StructField("c", IntegerType()),
                ]
            )

            sorted_data = [
                (col_a, col_b, col_c),
            ]
        elif sort_order == "desc":
            sorted_fields = StructType(
                [
                    StructField("c", IntegerType()),
                    StructField("b", IntegerType()),
                    StructField("a", IntegerType()),
                ]
            )

            sorted_data = [
                (col_c, col_b, col_a),
            ]
        else:
            raise ValueError(
                "['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'".format(
                    sort_order=sort_order
                )
            )

        unsorted_df = spark.createDataFrame(unsorted_data, unsorted_fields)
        expected_df = spark.createDataFrame(sorted_data, sorted_fields)

        return unsorted_df, expected_df

    unsorted_df, expected_df = _get_simple_test_dataframes(sort_order=sort_order)

    unsorted_df.printSchema()
    sorted_df = quinn.sort_columns(unsorted_df, sort_order)
    sorted_df.printSchema()
    expected_df.printSchema()
    chispa.schema_comparer.assert_schema_equality(
        sorted_df.schema, expected_df.schema, ignore_nullable=True
    )


def test_sort_struct_flat(spark):
    _test_sort_struct_flat(spark, "asc")


def test_sort_struct_flat_desc(spark):
    _test_sort_struct_flat(spark, "desc")


def test_sort_struct_nested(spark):
    def _get_test_dataframes() -> tuple[(DataFrame, DataFrame)]:
        unsorted_schema = StructType(
            [
                StructField("_id", StringType(), nullable=False),
                StructField("first_name", StringType(), nullable=False),
                StructField(
                    "address",
                    StructType(
                        [
                            StructField(
                                "zip",
                                StructType(
                                    [
                                        StructField(
                                            "last4", IntegerType(), nullable=True
                                        ),
                                        StructField(
                                            "first5", IntegerType(), nullable=True
                                        ),
                                    ]
                                ),
                                nullable=True,
                            ),
                            StructField("city", StringType(), nullable=True),
                        ]
                    ),
                    nullable=True,
                ),
            ]
        )

        sorted_schema = StructType(
            [
                StructField("_id", StringType(), nullable=False),
                StructField(
                    "address",
                    StructType(
                        [
                            StructField("city", StringType(), nullable=True),
                            StructField(
                                "zip",
                                StructType(
                                    [
                                        StructField(
                                            "first5", IntegerType(), nullable=True
                                        ),
                                        StructField(
                                            "last4", IntegerType(), nullable=True
                                        ),
                                    ]
                                ),
                                nullable=True,
                            ),
                        ]
                    ),
                    nullable=True,
                ),
                StructField("first_name", StringType(), nullable=False),
            ]
        )

        _id = "12345"
        city = "Fake City"
        zip_first5 = 54321
        zip_last4 = 12345
        first_name = "John"

        unsorted_data = [
            (_id, first_name, (((zip_last4, zip_first5)), city)),
        ]
        sorted_data = [
            (_id, ((city, (zip_first5, zip_last4))), first_name),
        ]

        unsorted_df = spark.createDataFrame(unsorted_data, unsorted_schema)
        expected_df = spark.createDataFrame(sorted_data, sorted_schema)

        return unsorted_df, expected_df

    unsorted_df, expected_df = _get_test_dataframes()

    unsorted_df.printSchema()
    sorted_df = quinn.sort_columns(unsorted_df, "asc")
    sorted_df.printSchema()

    # TODO: work on assert_schema_equality to handle nested structs
    assert True
    # assert_schema_equality(sorted_df, expected_df)


# create a local spark session
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("abc").getOrCreate()
# test_sort_struct(spark)
