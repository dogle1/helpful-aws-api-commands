import boto3
from boto3.dynamodb.types import TypeDeserializer

def run_partisql_query(sql: str) -> list:
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.execute_statement(Statement=sql)
    records = response['Items']

    # Query the remaining records that could not fit in the first query.
    next_token = response.get('NextToken', None)
    while next_token:
        temp_len = len(records)
        response = dynamodb.execute_statement(Statement=sql, NextToken=next_token)
        records.extend(response['Items'])
        next_token = response.get('NextToken', None)

    return records

def parse_ddb_data(ddb_records: list) -> list:
    """This function will take in a list of dictionaries returned from AWS DynamoDB and parse out the nested dictionaries where the type key is specified.
        EX: [{index: {'S': '606079871610#2022#1##AmazonEC2'}},...] -> [{index : '606079871610#2022#1##AmazonEC2'}, ...]
    """
    parsed_ddb_records = []
    # Remove DDB formatting from the dictionaries and parse values into proper data types.
    for record in ddb_records:
        deserializer = TypeDeserializer()
        python_data = {k: deserializer.deserialize(v) for k, v in record.items()}
        parsed_ddb_records.append(python_data)
    return parsed_ddb_records
