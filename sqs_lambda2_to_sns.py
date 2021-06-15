import json
import boto3
import time

DB = boto3.resource("dynamodb")
region = 'us-east-2'
dynamodb = boto3.client('dynamodb', region_name=region)
table = DB.Table("Phone_Msg")
sns = boto3.client('sns')

def send_sms(phone, msg):
    print('send sms')
    sns.publish(
        PhoneNumber=phone,
        Message=msg
        )



def check_dates(old, new):
    """
    :param old:
    :param new:
    :return:
        True if the diff between old and new is 24 hours (86400 seconds) or more (we should send SMS), else False
    """
    return int(new) - int(old) >= 24 * 60 * 60


def update_contact(table_name, phone, msg, curr_time=None):
    update = "set #msg = :new_msg"
    exps = {
        ':new_msg': {
            'S': msg
        }
    }
    expNames = {'#msg': 'message'}
    if curr_time:
        update = f'{update}, #dt = :new_date'
        exps[':new_date'] = {'S': curr_time}
        expNames['#dt'] = 'lastMessage'
    res = dynamodb.update_item(
        TableName=table_name,
        Key={
            'number': {'S': phone}
        },
        ExpressionAttributeNames=expNames,
        UpdateExpression=update,
        ExpressionAttributeValues=exps
    )


def put_item(table_name, phone, msg, curr_time):
    dynamodb.put_item(
        TableName=table_name,
        Item={
            'number': {'S': phone},
            'message': {'S': msg},
            'lastMessage': {'S': curr_time}
        })


def deal_with_contact(table_name, phone, msg, curr_time):
    """
    checks if the phone exists, if not adds it, if yes update the message and check for date
    :param table_name:
    :param phone:
    :param msg:
    :param curr_time:
    :return:
        returns True in case we should send SMS, False if not
    """
    try:
        res = table.get_item(
            Key={'number': phone}
        )

        if 'Item' in res:
            # todo: update the message field
            last_sent = res['Item']['lastMessage']
            to_send = check_dates(last_sent, curr_time)
            update_contact(table_name, phone, msg, curr_time if to_send else None)
            # compare both dates
            return to_send

        # not in the table, add it and return True
        put_item(table_name, phone, msg, curr_time)
        return True

    except Exception as e:
        print(e)
        raise (e)


def lambda_handler(event, context):
    # this lambda is triggered when receiving something in SQS, into the event var
    now = str(int(time.time()))
    # reads the data recieved from SQS
    data = json.loads(json.loads(event['Records'][0]['body'])['responsePayload']['body'])

    for contact in data:
        phone, msg = contact.values()
        phone = f"+972{phone[1:]}"
        if deal_with_contact('Phone_Msg', phone, msg, now):
            send_sms(phone, msg)
            #print(f"send SMS to {phone}")

    return {
        'statusCode': 200,
        'body': json.dumps('ALL good!')
    }