import boto3

# Initialize Athena and Glue clients
athena = boto3.client('athena')
glue = boto3.client('glue')

# Specify the database name
database_name = 'bootcamp'

# List the table names in the specified database using Glue
table_names = glue.get_tables(DatabaseName=database_name)['TableList']

# Initialize an array to store table names
table_name_array = []

# Iterate through the table names and append them to the array
for table in table_names:
    table_name = table['Name']
    table_name_array.append(table_name)
    
# Iterate through the table names and run SHOW CREATE TABLE for each table using Athena
for table_name in table_name_array:
    show_create_table_query = f"SHOW CREATE TABLE {database_name}.{table_name}"

    # Start the Athena query execution
    result = athena.start_query_execution(
        QueryString=show_create_table_query,
        ResultConfiguration={"OutputLocation": "s3://xxxxxxx"}
    )

    # Get the query execution ID
    query_execution_id = result['QueryExecutionId']

    # Wait for the query to complete
    while True:
        query_execution_response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_execution_response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    # Get and print the DDL statement for the table
    query_results = athena.get_query_results(QueryExecutionId=query_execution_id)
    
    # Extract the DDL statement from the query results
    ddl_statement = ""
    for row in query_results['ResultSet']['Rows']:
        if 'VarCharValue' in row['Data'][0]:
            ddl_statement += row['Data'][0]['VarCharValue'] + '\n'

    print(f"DDL statement for table {table_name}:\n{ddl_statement}")
