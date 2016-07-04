# -*- coding: utf-8 -*-
"""
Created on Sat Jul  2 14:13:10 2016

@author: neelima
"""

import math
import pyspark as py
import numpy as np
import pandas as pd
import pickle

def func_format(row):
    return row.strip("><").split("><")

def data_cleansing():
    df = sq.read.format('com.databricks.spark.csv')\
            .option('header', 'true')\
            .option('mode', 'FAILFAST')\
            .option('delimiter', '|')\
            .option('inferSchema', True)\
            .load(link_posts).select('PostTypeId', 'Tags', 'OwnerUserId', 'ParentId', 'Id', 'Score').cache()
    sq.registerDataFrameAsTable(df, "posts_table")
    question_tag_association = sq.sql('''SELECT Tags, Id FROM posts_table WHERE PostTypeId = 1''')
    question_tag_association = question_tag_association.map(lambda row: (func_format(row[0]), row[1]))\
                                                   .toDF().withColumnRenamed('_1', 'Tags')\
                                                   .withColumnRenamed('_2', 'Id')
    answers_score = sq.sql('''SELECT OwnerUserId, ParentId, Score FROM posts_table WHERE PostTypeId = 2''')
    user_tag_score = answers_score.join(question_tag_association, answers_score.ParentId == question_tag_association.Id, 'left_outer')\
                              .drop(question_tag_association.Id)
    user_tag_score.select('OwnerUserId', 'ParentId', 'Score', 'Tags').write \
                .format('com.databricks.spark.csv') \
                .option('delimiter', '|')\
                .save('/root/user_tag_score.csv')


def score_percentile(score_list, n1 = 0.1, n2 = 0.9):
    '''INPUT: list of score
       OUTPUT: return n1, n2 percentile of score
    '''
        # return k, a
    pass


def sigmoid(x, k, a):
    '''INPUT: score
       OUTPUT: sigmoid score
    '''
    return 1/(1 + math.exp(k*(a - x)))


def user_expertise(user_tag_score):
    user_tag_expertise = {}
    for row in user_tag_score.iterrows():
        for tag in row[1]['Tags'].strip('[]').replace("'","").split(', '):
            tag_expertise = user_tag_expertise.get(row[1]['OwnerUserId'], {})
            tag_expertise[tag] = tag_expertise.get(tag, 0) + sigmoid(row[1]['Score'])
            user_tag_expertise[row[1]['OwnerUserId']] = tag_expertise
    return user_tag_expertise

with open('/tmp/user_tag_expertise.pickle', 'wb') as handle:
pickle.dump(user_tag_expertise, handle)
