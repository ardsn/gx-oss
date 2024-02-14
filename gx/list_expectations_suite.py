import great_expectations as gx

context = gx.get_context()
print(context.list_expectation_suite_names())