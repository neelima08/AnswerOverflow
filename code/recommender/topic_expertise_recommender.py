# coding: utf-8

import pandas as pd
import numpy as np
import pickle
from datetime import datetime

questions_answerers = pd.read_csv('/tmp/Question_Answerer.csv', delimiter='|')
questions_answerers['OwnerUserIdList'] = questions_answerers['OwnerUserId'].astype(list)
questions_answerers_list = questions_answerers.drop(['OwnerUserId'], axis=1)
questions_answerers_dict = questions_answerers_list.set_index('ParentId').to_dict()

def func_format(row):
    return row.strip("><").split("><")

posts = pd.read_csv('/tmp/posts.csv', delimiter='|', usecols= ['Id', 'PostTypeId', 'Tags'])

questions = posts.loc[posts['PostTypeId'] == 1]
questions['Tags'] = questions['Tags'].apply(func_format)

questions_tags = questions.drop(['PostTypeId'], axis=1)
questions_tags_dict = questions_tags.set_index('Id').to_dict()

with open('/tmp/clusters_expertise.pickle', 'r') as handle:
    clusters_expertise = pickle.load(handle)

with open('/tmp/tags_clusters.pickle', 'r') as handle:
    tags_clusters = pickle.load(handle)

sampling = 1.0

cluster_to_potential_answerers = {}
for cluster in clusters_expertise.keys():
    users = clusters_expertise[cluster]
    number_users = int(sampling*len(users))

    potential_answerers = set()
    for user_score in users[:number_users]:
        potential_answerers.add(user_score[0])

    cluster_to_potential_answerers[cluster] = potential_answerers

total = 0
success = 0

for question in questions_tags_dict['Tags'].keys():
    if question not in questions_answerers_dict['OwnerUserIdList']:
        continue

    total += 1
    answerers = set(map(int, questions_answerers_dict['OwnerUserIdList'][question].strip('[]').split(', ')))

    tags = questions_tags_dict['Tags'][question]

    found = False
    for tag in tags:
      if tag not in tags_clusters:
            continue

        potential_answerers = cluster_to_potential_answerers[cluster]
        if len((potential_answerers).intersection(answerers)) > 0:
            found = True
            break
    if found:
        success += 1

print (success, total)
