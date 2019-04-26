import json
import boto3
import decimal


# Helper class to convert a DynamoDB item to JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def scan_table_allpages(table, filter_key=None, filter_value=None):
    """
    Perform a scan operation on table.
    Can specify filter_key (col name) and its value to be filtered.
    This gets all pages of results. Returns list of items.
    """

    if filter_key and filter_value:
        filtering_exp = Key(filter_key).eq(filter_value)
        response = table.scan(FilterExpression=filtering_exp)
    else:
        response = table.scan()

    items = response['Items']
    while True:
        # print(len(response['Items']))
        if response.get('LastEvaluatedKey'):
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items += response['Items']
        else:
            break

    return items


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Bips')

response = scan_table_allpages(table)

with open('datasets/dynamodb_20180823.json', 'w', encoding='utf-8') as outfile:
    outfile.write(json.dumps(response, cls=DecimalEncoder, ensure_ascii=False))
