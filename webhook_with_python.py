#!/usr/bin/python
# Imports

import json
import os
from sqlalchemy import create_engine
from sqlalchemy.types import Date
from sqlalchemy.types import DateTime
import pandas as pd
from datetime import datetime

# RDS settings

RDS_USERNAME = 'RDS_USERNAME'
RDS_USER_PWD = 'RDS_USER_PWD'
RDS_HOST = 'RDS_HOST'
RDS_DB_PORT = 'RDS_DB_PORT'
RDS_DB_NAME = 'RDS_DB_NAME'


# Define a NPS Sentiment about the rating

def getNpsSentiment(hs_feedback_last_nps_rating):
    result = 'passivo'
    try:
        npsRating = int(hs_feedback_last_nps_rating)
        if npsRating >= 9:
            result = 'promotor'
        elif npsRating >= 7:
            result = 'passivo'
        elif npsRating is None:
            result = None
        else:
            result = 'detrator'
    except:
        result = None
    return result

# Check if a value exists and then return it. Otherwise, returns None.


def getValue(dict, firstLevelK, secondLevelK=None, ThirdLevelK=None, FourthLevelK=None,):
    result = None
    try:
        if secondLevelK is None:
            result = dict[firstLevelK]
        elif ThirdLevelK is None:
            result = dict[firstLevelK][secondLevelK]
        elif FourthLevelK is None:
            result = dict[firstLevelK][secondLevelK][ThirdLevelK]
        else:
            result = \
                dict[firstLevelK][secondLevelK][ThirdLevelK][FourthLevelK]
    except:
        print ('key(s) not found.')
    return result


def lambda_handler(event, context):
    POSTGRES = "postgresql://"+RDS_USERNAME+":" \
        + RDS_USER_PWD + "@" + RDS_HOST + ":" + RDS_DB_PORT + "/" + RDS_DB_NAME
    engine = create_engine(POSTGRES, pool_pre_ping=True)
    result = dict()
    try:
        if 'body' in event:
            jsonDict = json.loads(event['body'])
        else:
            jsonDict = event
        result.update(dict([
            ('feedback_id', getValue(jsonDict, 'response', 'id')),
            ('feedback_received_at', datetime.now().__str__()),
            ('responder_email', getValue(jsonDict, 'response', 'user',
             'email')),
            ('responder_name', getValue(jsonDict, 'response', 'user',
             'name')),
            ('responder_company', getValue(jsonDict, 'traits', 'company')), 
            ('responder_plan', getValue(jsonDict, 'traits', 'plan')), 
            ('responder_role', getValue(jsonDict, 'traits', 'role')),  
            ('referrer', getValue(jsonDict, 'response', 'referrer')),
            ('event', getValue(jsonDict, 'event')),
            ('dismissed', getValue(jsonDict, 'response', 'dismissed')),
            ('feedback_date', getValue(jsonDict, 'response', 'created')),
            ('comment', getValue(jsonDict, 'response', 'feedback')),
            ('score', getValue(jsonDict, 'response', 'rating')),
            ('feedback_sentiment', getNpsSentiment(getValue(jsonDict,
             'response', 'rating'))),
            ]))
        if result['feedback_id'] is not None:
            dfToSave = pd.DataFrame.from_dict(result, orient='index').transpose()
            dfToSave.to_sql('nps', engine, index=False, if_exists='append', method='multi', dtype={'feedback_received_at': DateTime(timezone=False), 'feedback_date': DateTime(timezone=False)})
        else:
            result = {'msg': 'Failed to read values'}
    except Exception as e:
        result['msg'] = str(e)
    finally:
        try:
            result = {
                "statusCode": 200,
                "body": json.dumps(result)
            }
        except Exception as ef:
            result['msg'] = str(ef)

    return result
