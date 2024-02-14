import great_expectations as gx
from great_expectations.core.expectation_configuration import (
    ExpectationConfiguration
)

context = gx.get_context()

suite = context.add_or_update_expectation_suite(
    expectation_suite_name='my_suite'
)

# Create an Expectation
expectation_configuration_1 = ExpectationConfiguration(
    # Name of expectation type being added
    expectation_type="expect_table_columns_to_match_ordered_list",
    # These are the arguments of the expectation
    # The keys allowed in the dictionary are Parameters and
    # Keyword Arguments of this Expectation Type
    kwargs={
        "column_list": [
            "account_id",
            "user_id",
            "transaction_id",
            "transaction_type",
            "transaction_amt_usd",
        ]
    },
    # This is how you can optionally add a comment about this expectation.
    # It will be rendered in Data Docs.
    # See this guide for details:
    # `How to add comments to Expectations and display them in Data Docs`.
    meta={
        "notes": {
            "format": "markdown",
            "content": "Some clever comment about this expectation. **Markdown** `Supported`",
        }
    },
)
# Add the Expectation to the suite
suite.add_expectation_configurations([expectation_configuration_1])

context.save_expectation_suite(expectation_suite=suite)
