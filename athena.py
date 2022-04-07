import boto3


def get_query_results(query_execution_id: str, profile_name: str) -> list:
    """This function will return the results of the specified Athena query. It will automatically handle paginating the data for the query if it is needed.

        Args:
            query_execution_id (str): The unique id of the Athena query that has been executed.
            profile_name (str): The local AWS CLI profile name from which to pull the credentials to run the AWS operations.

        Returns:
            list: This will be all of the data for the query returned from AWS Athena.
    """
    
    session = boto3.session.Session(profile_name=profile_name)
    athena_client = session.client('athena')
    response = athena_client.get_query_results(
        QueryExecutionId=query_execution_id    
    )   
    next_token = response.get('NextToken', None)
    results = response
    # Retrieve the remaining data, if NextToken was returned from AWS.
    while next_token != None:
        response = athena_client.get_query_results(
            NextToken=next_token
        )   
        # Append the rows from the new response to the previous rows. Ignore the first row since it houses the column names.
        results['ResultSet']['Rows'].extend(response['ResultSet']['Rows'][1:])
        next_token = response.get('NextToken', None)
        
    return results

def convert_list_of_dict_to_list(in_list: list) -> list:
    """This function will take in a list of dictionaries, commonly returned from AWS Athena queries and convert them into a list of just the values.
    
        Args:
            in_list (list): A list of dictionaries returned from AWS Athena queries.
            EX: [{'VarCharValue': 'id'}, {'VarCharValue': 'account_id'}, ...]

        Returns:
            list: A list of just the values from the received list.
            EX: ['id', 'account_id',...]
    """
    
    output = []
    for item in in_list:
        # Convert the dictionaries values into a list
        value_list = list(item.values())
        # Catch null row values.
        try:
            value = value_list[0]
        except IndexError:
            value = None
        # Store value in the output list
        output.append(value)
    return output
    
def format_athena_query_data_to_dict(athena_query_data: dict) -> list:
    """This function will take in the raw response of an AWS Athena Query and convert it to a list of dictionaries where the keys are the column names
        and the values are the values for each corresponding column in each row. 

    Args:
        athena_query_data (dict): A response from an AWS Athena query.

    Returns:
        _type_: _description_
    """
    # Convert athena columns into a list 
    row_columns = athena_query_data['ResultSet']['Rows'][0]['Data']
    row_columns = convert_list_of_dict_to_list(row_columns)
    
    row_dicts = athena_query_data['ResultSet']['Rows'][1:]
    output_list = []
    for row in row_dicts:
        # Convert row data from nested dictionary to list
        temp_row = row['Data']
        temp_row_data = convert_list_of_dict_to_list(temp_row)
        # Convert to dictionary with columns as keys and row data as values
        temp_formatted_row = {}
        for i in range(len(temp_row_data)):
            temp_formatted_row[row_columns[i]] = temp_row_data[i]
        output_list.append(temp_formatted_row)

    return output_list


