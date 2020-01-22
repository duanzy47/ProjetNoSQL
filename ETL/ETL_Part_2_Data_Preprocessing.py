# -*- coding: utf-8 -*-
"""
GDELT BIG DATA DATA PREPROCESSING PROJECT
ETL Pipeline - Part 2 - Big Data Preprocessing

Note: Tested in Python 2.7 on AWS EMR Zeppelin

Requirements:

sudo pip install pandas
sudo pip install boto3
sudo pip install pymongo
"""

### Packages
import numpy as np
import time
import timeit
import pandas as pd
import pymongo as mb

from pprint import pprint

import io
import boto3

########################################################

def s3_to_pandas(bucket_name, file_name, columns):
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), compression='zip',
                     error_bad_lines=True, header=None, index_col=None,
                     sep='\t')
    df.columns = columns
    return df

def get_article_mention_language(translateInfo):
    """ For GDELT Mentions
    """
    if translateInfo=='':
        language = 'eng'
    else:
        language = translateInfo.split(';', 1)[0][-3:]
    return language
    
def preprocess_gdelt_gkg_rows(row):
    """ For GDELT GKG
    """
    # Split String Lists
    split_with_semicolon = ['AllNames', 'Amounts', 'Counts', 'Locations',
                            'Organizations', 'Persons', 'Quotations',
                            'RelatedImages', 'Themes', 'V2Counts',
                            'V2Locations', 'V2Organizations', 'V2Persons',
                            'V2Themes']
    
    split_with_comma = ['GCAM', 'V2Tone']
    
    # General Splits
    for key_name in split_with_semicolon:
        # Transform string into list
        row[key_name] = row[key_name].split(';')
        
        # Remove elements with empty strings
        row[key_name] = [el for el in row[key_name] if el != '']
    
    for key_name in split_with_comma:
        # Transform string into list
        row[key_name] = row[key_name].split(',')
        
        # Remove elements with empty strings
        row[key_name] = [el for el in row[key_name] if el != '']
    
    return row

def preprocess_gdelt_gkg_tone(x):
    """ For GDELT GKG
    """
    res = dict()
    res['tone'] = float(x[0])
    res['positive_score'] = x[1]
    res['negative_score'] = x[2]
    res['polarity'] = x[3]
    res['activity_reference_density'] = x[4]
    res['group_reference_density'] = x[5]
    return res

def preprocess_gdelt_gkg_location(x):
    """ For GDELT GKG
    """
    res_list = []
    # print(x)
    for location in x:
        x_split = location.split('#')
        res = dict()
        res['type'] = x_split[0]
        res['full_name'] = x_split[1]
        res['country_code'] = x_split[2]
        res['adm1_code'] = x_split[3]
        res['latitude'] = x_split[4]
        res['longitude'] = x_split[5]
        res['featureID'] = x_split[6]
        res_list.append(res)
        
    return res_list


########################################################
# STEP 1: Setup S3 Bucket and MongoDB EC2 Connection
########################################################

### (i) Setup Python Connection to S3 Bucket
AWS_ID = "*****************"
AWS_KEY = "*****************"

s3 = boto3.client('s3', aws_access_key_id=AWS_ID,
                  aws_secret_access_key=AWS_KEY)

# Test S3 Bucket Connnection
response = s3.list_buckets()

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print bucket['Name']

### (ii) Setup Python Connection to MongoDB EC2 Server
HOST_SERVER = ""
mongo_client = mb.MongoClient(HOST_SERVER)

# Test EC2 Connection
print(mongo_client.list_database_names())

# Create MongoDB Database
msbd_database = mongo_client['MSBD_2019_2020']

# Create MongoDB Collection in Database ("SQL Table")
gdelt_main_collection = msbd_database['gdelt_main']
gdelt_gkg_collection = msbd_database['gdelt_gkg']

print(msbd_database.list_collection_names())    

########################################################
# STEP 2: Data Preprocessing
########################################################

### (i) Get Column Description for GDELT Tables
GDELT_EVENTS_INFO = '''https://raw.githubusercontent.com/linwoodc3/gdelt2HeaderRows/master/'''+ \
                     '''schema_csvs/GDELT_2.0_Events_Column_Labels_Header_Row_Sep2016.csv'''
GDELT_MENTIONS_INFO = '''https://raw.githubusercontent.com/linwoodc3/gdelt2HeaderRows/master/'''+ \
                       '''schema_csvs/GDELT_2.0_eventMentions_Column_Labels_Header_Row_Sep2016.tsv'''
GDELT_GKG_INFO = '''https://raw.githubusercontent.com/linwoodc3/gdelt2HeaderRows/master/'''+ \
                  '''schema_csvs/GDELT_2.0_gdeltKnowledgeGraph_Column_Labels_Header_Row_Sep2016.tsv'''

gdelt_events_descrip = pd.read_csv(GDELT_EVENTS_INFO, sep=',')
gdelt_mentions_descrip = pd.read_csv(GDELT_MENTIONS_INFO, sep='\t')
gdelt_gkg_descrip = pd.read_csv(GDELT_GKG_INFO, sep='\t')

gdelt_events_cols = gdelt_events_descrip['tableId'].tolist()
gdelt_mentions_cols = gdelt_mentions_descrip.iloc[:16,1].tolist()
gdelt_gkg_cols = gdelt_gkg_descrip['tableId'].tolist()

#### (ii) Get List of all available GDELT Files in S3 Bucket
BUCKET_NAME = "*****************"
s3_resource = boto3.resource('s3')
gdelt_storage_bucket = s3_resource.Bucket(BUCKET_NAME)

#### (iii) Inserting GDELT Mentions into MongoDB
successful_mongodb_insertions, failed_mongodb_insertions = [], []

for i, file in enumerate(gdelt_mentions_files):
    
    # Check progress
    if i % 10 == 0:
        print i
    
    try:
        # Read Pandas DataFrame From S3
        temp_df = s3_to_pandas(bucket_name=BUCKET_NAME, file_name=file,
                               columns=gdelt_mentions_cols)
        
        # Preprocess Pandas DataFrame
        temp_df = temp_df.copy()
        temp_df['MentionDocTranslationInfo'] = temp_df['MentionDocTranslationInfo'].fillna('')
        temp_df['MentionDocOriginalLanguage'] = \
            temp_df['MentionDocTranslationInfo'].apply(lambda x: get_article_mention_language(x))
        
        # Prepare for Insertion into MongoDB
        pandas_to_mongo_mentions_data = temp_df.copy()
        pandas_to_mongo_mentions_data = pandas_to_mongo_mentions_data.fillna('')
        pandas_to_mongo_mentions_data = pandas_to_mongo_mentions_data.to_dict('records')
        
        # Insert into MongoDB
        _ = gdelt_main_collection.insert_many(pandas_to_mongo_mentions_data)
        successful_mongodb_insertions.append(file)
        
    except:
        print 'Failure with file ' + str(i)
        failed_mongodb_insertions.append(file)

# Test if everything works fine
print(gdelt_main_collection.count_documents(filter={}))
# 600 000 documents for 1 day of GDELT