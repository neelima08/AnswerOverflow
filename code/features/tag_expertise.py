import math
import pyspark as py
import numpy as np
import pandas as pd
import pickle

def xml2List(row):
    """
    - Converts XML elements to list. Example: <foo><bar> will become [foo, bar]
    INPUT: Elements in XML format
    OUTPUT: Elements in list format
    """
    return row.strip("><").split("><")

def get_user_tags_score_mapping(s3_url):
    """
    - Returns a data frame which consists of 3 columns:
    1) UserId
    2) List of tags associated with the question answered by user
    3) Score of the answer
    """
    # Read posts file
    posts = sq.read.format('com.databricks.spark.csv')\
            .option('header', 'true')\
            .option('mode', 'FAILFAST')\
            .option('delimiter', '|')\
            .option('inferSchema', True)\
            .load(link_posts).select('PostTypeId', 'Tags', 'OwnerUserId', 'ParentId', 'Id', 'Score').cache()

    # Get questions data frame consisting of two columns: 1) ID, 2) List of tags
    sq.registerDataFrameAsTable(df, "posts_table")
    question_tag_association = sq.sql('''SELECT Tags, Id FROM posts_table WHERE PostTypeId = 1''')
    question_tag_association = question_tag_association.map(lambda row: (xml2List(row[0]), row[1]))\
                                                   .toDF().withColumnRenamed('_1', 'Tags')\
                                                   .withColumnRenamed('_2', 'Id')

    # Get answerns data frame
    answers_score = sq.sql('''SELECT OwnerUserId, ParentId, Score FROM posts_table WHERE PostTypeId = 2''')

    # Join questions and answers data frame on question ID
    return answers_score.join(question_tag_association, answers_score.ParentId == question_tag_association.Id, 'left_outer')\
                              .drop(question_tag_association.Id)

def save_user_tags_score_mapping(user_tag_score, filename):
    user_tag_score.select('OwnerUserId', 'ParentId', 'Score', 'Tags').write \
                .format('com.databricks.spark.csv') \
                .option('delimiter', '|')\
                .save(filename)

def sigmoid(x, k, a):
    return 1 / (1 + math.exp(k * (a - x)))


def get_user_expertise(user_tag_score):
    """
    - Returns a dictionary which conists of a mapping from UserId to scores associated with each tag 
    """
    user_tag_expertise = {}

    for row in user_tag_score.iterrows():
        user = row[1]['OwnerUserId']
        score = row[1]['Score']
        tags = row[1]['Tags'].strip('[]').replace("'","").split(', ')

        for tag in tags:
            tag_expertise = user_tag_expertise.get(user, {})
            tag_expertise[tag] = tag_expertise.get(tag, 0) + sigmoid(score, 0.2, 20)
            user_tag_expertise[user] = tag_expertise
    
    return user_tag_expertise

def save_user_expertise(filename):
    with open(filename, 'wb') as handle:
        pickle.dump(user_tag_expertise, handle)

if __name__ == '__main__':
    # get data from s3
    s3_url = "s3n://project-neelima/posts.csv"
    user_tag_score = get_user_tags_score_mapping(s3_url)
    save_get_user_tags_score_mapping(user_tag_score, 'user_tag_score.csv')

    user_tag_expertise = get_user_expertise(user_tag_score)
    save_user_expertise(user_tag_expertise, 'user_tag_expertise.pickle')
