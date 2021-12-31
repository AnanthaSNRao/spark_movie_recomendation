import boto3
import csv
import json

def lambda_handler(event, context):
    region = 'us-east-2'
    reList = []
    # return 'dsxsdgfsdfgsd'
    try:
        s3 = boto3.client('s3', region_name= region)
        ddb = boto3.client('dynamodb', region_name = region)
        confile = s3.get_object(Bucket = 'mldatasetspark', Key = 'ratings.csv')
        temp = confile['Body'].read().decode()
       
        reList = temp.split('\n')
        firstrecord= True
       
        csv_reader = csv.reader(reList, delimiter=',')
        # return json.loads(json.dumps(type("posting"), default=str))
        id = 0
        for row in csv_reader:
            if firstrecord:
                firstrecord = False
                continue
            id+=1
            userid = row[0]
            moveid = row[1]
            rating = row[2]
            timestamp = row[3]
            response = ddb.put_item(
                TableName = 'ratings1',
                Item={
                    'id':{'N': str(id)},
                    'userid':{'N': userid},
                    'movieid': {'N': moveid},
                    'rating': {'N': rating},
                    }
                )
        return json.loads(json.dumps("PUT", default=str))
            
    except Exception as e:
        return json.loads(json.dumps(e, default=str))