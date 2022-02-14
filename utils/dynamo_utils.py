import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb')


def insert_item(table, item):
    """
    insert item into dynamo table
    :param table: (string)
    :param item: (dict) keys are columns, values are contents of columns
    :return:
    """
    table = dynamodb.Table(table)
    response = table.put_item(Item=item)
    return response


def describe_table(table):
    return client.describe_table(TableName=table)


def get_item(table, key):
    """

    :param table: (string)
    :param key: (dict)
    :return:
    """
    table = dynamodb.Table(table)
    response = table.get_item(
        Key=key
    )
    item = response['Item']
    return item


def get_all_items(table):
    """
    :param table: string
    :return: list[dict]
    """
    table = dynamodb.Table(table)
    # Get all data, will take a while to get all the data
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data


def query_by_key(table, key, condition, compare_value):
    """
    Filter by primary key
    :param table: (string)
    :param key: (string) primary key name
    :param condition: (string) eq, gt, gte, lt, lte
    :param compare_value: (string)
    :return: list(dict)

    example query_by_key('secutix-test-client-table', key='type', condition='eq', compare_value='/client/order')
    """
    conditions = {'eq': 'Equal', 'gt': 'GreaterThan', 'gte': 'GreaterThanEqual', 'lt': 'LessThan',
                  'lte': 'LessThanEqual'}
    if condition not in conditions.keys():
        message = '\n\t\t\t\t'
        for key, value in conditions.items():
            message += key + ' for ' + value + '\n\t\t\t\t'
        raise ValueError('Condition must be the following value ' + message)

    table = dynamodb.Table(table)
    response = table.query(
        KeyConditionExpression=getattr(Key(key), condition)(compare_value)
    )

    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression=getattr(Key(key), condition)(compare_value),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
    return data


def query_by_key_between_value(table, key, low_value, high_value):
    """
    Filter primary key by a range
    :param table: (string)
    :param key: (string)
    :param low_value:
    :param high_value:
    :return: list(dict)

    example query_by_key_between_value('secutix-test-client-table', key='id', low_value=1, high_value=10)
    """
    table = dynamodb.Table(table)
    response = table.query(
        KeyConditionExpression=Key(key).between(low_value=low_value, high_value=high_value)
    )

    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression=Key(key).between(low_value=low_value, high_value=high_value),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
    return data


def query_by_attr(table, attr, condition, compare_value):
    """
    Filter by attribute
    :param table: (string)
    :param attr: (string)
    :param condition: (string)
    :param compare_value:
    :return: list(dict)
    example query_by_attr('secutix-test-client-table', key='timestamp', condition='gt', compare_value='2020-01-01')
    """
    conditions = {'eq': 'Equal', 'gt': 'GreaterThan', 'gte': 'GreaterThanEqual', 'lt': 'LessThan',
                  'lte': 'LessThanEqual'}
    if condition not in conditions.keys():
        message = '\n\t\t\t\t'
        for key, value in conditions.items():
            message += key + ' for ' + value + '\n\t\t\t\t'
        raise ValueError('Condition must be the following value ' + message)

    table = dynamodb.Table(table)
    response = table.scan(
        FilterExpression=getattr(Attr(attr), condition)(compare_value)
    )

    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=getattr(Attr(attr), condition)(compare_value),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
    return data


def query_by_attr_between_value(table, attr, low_value, high_value):
    """

    :param table: (string)
    :param attr: (string)
    :param low_value:
    :param high_value:
    :return:
    example query_by_attr_between_value('secutix-test-client-table', key='id', low_value=1, high_value=10)
    """
    table = dynamodb.Table(table)
    response = table.scan(
        FilterExpression=Attr(attr).between(low_value=low_value, high_value=high_value)
    )

    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=Attr(attr).between(low_value=low_value, high_value=high_value),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
    return data


def query_custom_condition(table, condition):
    table = dynamodb.Table(table)
    response = table.scan(
        FilterExpression=condition
    )

    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=condition,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
    return data


def update_item(table, key, attributes):
    """
    Example
            update_item(table='secutix-test-client-table',
                        key={'type': '/test-client/order', 'timestamp': '2020-12-15T13:36:44.470Z'},
                        attributes={'data': 'new values'})

    Update item with new attribute - value
    :param table: (string) table name
    :param key: (dict) primary key/sort key value to update
    :param attributes: (dict) attributes keys/values to update
    :return:

    """
    table = dynamodb.Table(table)
    ConditionExpression = []
    ExpressionAttributeNames = {}
    UpdateExpression = []
    ExpressionAttributeValues = {}

    for index, k in enumerate(key):
        ConditionExpression.append(f'#{k}{index} = :keyval{index}')
        ExpressionAttributeValues[f':keyval{index}'] = key[k]
        ExpressionAttributeNames[f'#{k}{index}'] = k

    for index, k in enumerate(attributes):
        UpdateExpression.append(f'#{k}{index} = :val{index}')
        ConditionExpression.append(f'#{k}{index} <> :val{index}')

        ExpressionAttributeValues[f':val{index}'] = attributes[k]
        ExpressionAttributeNames[f'#{k}{index}'] = k

    UpdateExpression = ' '.join(UpdateExpression)
    ConditionExpression = ' AND '.join(ConditionExpression)
    try:
        resp = table.update_item(
            Key=key,
            UpdateExpression=f'SET {UpdateExpression}',
            ExpressionAttributeValues=ExpressionAttributeValues,
            ExpressionAttributeNames=ExpressionAttributeNames,  # prevent reserved keyword
            ConditionExpression=ConditionExpression,
            ReturnValues='UPDATED_NEW'
        )
    except client.exceptions.ConditionalCheckFailedException:
        raise ValueError('Key not exists')

    return resp


def insert_item_update(table, key, attributes):
    """
    Example
            insert_item_update(table='secutix-test-client-table',
                        key={'type': '/test-client/order', 'timestamp': '2020-12-15T13:36:44.470Z'},
                        attributes={'data': 'new values'})

    Update item with new attribute - value
    :param table: (string) table name
    :param key: (dict) primary key/sort key value to update
    :param attributes: (dict) attributes keys/values to update
    :return:

    """
    table = dynamodb.Table(table)

    ExpressionAttributeNames = {}
    UpdateExpression = []
    ExpressionAttributeValues = {}

    for index, k in enumerate(attributes):
        UpdateExpression.append(f'#{k}{index} = :val{index}')
        ExpressionAttributeValues[f':val{index}'] = attributes[k]
        ExpressionAttributeNames[f'#{k}{index}'] = k

    UpdateExpression = ' '.join(UpdateExpression)

    try:
        resp = table.update_item(
            Key=key,
            UpdateExpression=f'SET {UpdateExpression}',
            ExpressionAttributeValues=ExpressionAttributeValues,
            ExpressionAttributeNames=ExpressionAttributeNames,  # prevent reserved keyword
            ReturnValues='UPDATED_NEW'
        )
    except client.exceptions.ConditionalCheckFailedException:
        raise ValueError('Key not exists')

    return resp
