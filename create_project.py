from great_expectations.data_context import FileDataContext

datacontext_path = '.'

context = FileDataContext.create(project_root_dir=datacontext_path)
