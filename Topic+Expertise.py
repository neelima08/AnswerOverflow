import pandas as pd
import numpy as np

def func_format(row):
    return row.strip("><").split("><")

df_posts = pd.read_csv('/tmp/posts.csv', delimiter='|', usecols= ['PostTypeId', 'OwnerUserId', 'ParentId', 'Id', 'Score', 'Tags'])

questions = df_posts.loc[df_posts['PostTypeId'] == 1]
question_tags_association = questions.drop(['PostTypeId', 'ParentId', 'OwnerUserId', 'Score'], axis = 1)
question_tags_association['Tags'] = question_tags_association['Tags'].apply(func_format)

answers = df_posts.loc[df_posts['PostTypeId'] == 2]
answers_score = answers.drop(['Tags', 'PostTypeId', 'Id'], axis = 1)
answers_score.dropna(inplace = True)

answers_score['ParentId'] = answers_score['ParentId'].astype(int)
answers_score['OwnerUserId'] = answers_score['OwnerUserId'].astype(int)

user_tag_score = pd.merge(answers_score, question_tags_association, left_on='ParentId', right_on='Id')
user_tag_score = user_tag_score.drop(['ParentId', 'Id'], axis = 1)
user_tag_score.to_csv('/tmp/user_tag_score.csv', sep='|')


user_tag_score['Tags'] = user_tag_score['Tags'].astype(str)

import math

def sigmoid(x):
  return 1 / (1 + math.exp(-x))

user_tag_expertise = {}
for row in user_tag_score.iterrows():
    for tag in row[1]['Tags'].strip('[]').replace("'","").split(', '):
        tag_expertise = user_tag_expertise.get(row[1]['OwnerUserId'], {})
        tag_expertise[tag] = tag_expertise.get(tag, 0) + sigmoid(row[1]['Score'])

        user_tag_expertise[row[1]['OwnerUserId']] = tag_expertise

import pickle
with open('/tmp/user_tag_expertise.pickle', 'wb') as handle:
    pickle.dump(user_tag_expertise, handle)
