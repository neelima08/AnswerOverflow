# coding: utf-8

# In[2]:

import pandas as pd
import numpy as np
import pickle
from datetime import datetime


# In[3]:

questions_answerers = pd.read_csv('/tmp/Question_Answerer.csv', delimiter='|')
questions_answerers['OwnerUserIdList'] = questions_answerers['OwnerUserId'].astype(list)
questions_answerers_list = questions_answerers.drop(['OwnerUserId'], axis=1)
questions_answerers_dict = questions_answerers_list.set_index('ParentId').to_dict()


# In[4]:

def func_format(row):
    return row.strip("><").split("><")


# In[5]:

posts = pd.read_csv('/tmp/posts.csv', delimiter='|', usecols= ['Id', 'CreationDate', 'PostTypeId', 'Tags'])

questions = posts.loc[posts['PostTypeId'] == 1]
questions['CreationDate'] = questions['CreationDate'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f"))

questions_2016 = questions[(questions.CreationDate.dt.year >= 2016)]
questions_2016['Hour'] = questions_2016['CreationDate'].dt.hour
questions_2016['Tags'] = questions_2016['Tags'].apply(func_format)


# In[6]:

with open('/tmp/hour_to_users.pickle', 'r') as handle:
    hour_to_users = pickle.load(handle)


# In[8]:

with open('/tmp/clusters_expertise.pickle', 'r') as handle:
    clusters_expertise = pickle.load(handle)


# In[9]:

with open('/tmp/tags_clusters.pickle', 'r') as handle:
    tags_clusters = pickle.load(handle)


# In[10]:

clusters_expertise_dict = {}
for cluster in clusters_expertise.keys():
    users_score = clusters_expertise[cluster]

    users_score_dict = {}
    for user_score in users_score:
        users_score_dict[user_score[0]] = user_score[1]

    clusters_expertise_dict[cluster] = users_score_dict


# In[11]:

hour_score_dict = {}
for hour in hour_to_users.keys():
    hour_users_score = hour_to_users[hour]

    hour_users_score_dict = {}
    for hour_user_score in hour_users_score:
        hour_users_score_dict[hour_user_score[0]] = hour_user_score[1]
    
    hour_score_dict[hour] = hour_users_score_dict


# In[12]:

hour_cluster_expertise_dict = {}
for hour in hour_score_dict.keys():
    hour_users_score = hour_score_dict[hour]
    hour_users = set(hour_users_score.keys())

    clusters_expertise_dict_per_hour = {}
    for cluster in clusters_expertise_dict.keys():
        cluster_users_score = clusters_expertise_dict[cluster]
        cluster_users = set(cluster_users_score.keys())

        users_topical_hour_expertise_list = []

        hour_cluster_users = cluster_users.union(hour_users)
        for user in hour_cluster_users:
            users_topical_hour_expertise = hour_users_score.get(user, 0) + cluster_users_score.get(user, 0)
            users_topical_hour_expertise_list.append((user, users_topical_hour_expertise))

        clusters_expertise_dict_per_hour[cluster] = users_topical_hour_expertise_list

    hour_cluster_expertise_dict[hour] = clusters_expertise_dict_per_hour


# In[14]:

import operator

for hour in hour_cluster_expertise_dict.keys():
    hour_cluster_expertise = hour_cluster_expertise_dict[hour]

    for cluster in hour_cluster_expertise.keys():
        hour_cluster_expertise[cluster].sort(key=operator.itemgetter(1), reverse=True)


# In[30]:
sampling = 0.6

hour_cluster_to_potential_answerers = {}
for hour in hour_cluster_expertise_dict.keys():
    hour_cluster_expertise = hour_cluster_expertise_dict[hour]

    cluster_potential_answerers = {}
    for cluster in hour_cluster_expertise.keys():
        users = hour_cluster_expertise[cluster]
        number_users = int(sampling*len(users))

        potential_answerers = set()
        for user_score in users[:number_users]:
            potential_answerers.add(user_score[0])

        cluster_potential_answerers[cluster] = potential_answerers

    hour_cluster_to_potential_answerers[hour] = cluster_potential_answerers


# In[31]:

total = 0
success = 0

for index, row in questions_2016.iterrows():
    question = row['Id']
    if question not in questions_answerers_dict['OwnerUserIdList']:
        continue

    total += 1
    answerers = set(map(int, questions_answerers_dict['OwnerUserIdList'][question].strip('[]').split(', ')))

    tags = row['Tags']
    hour = row['Hour']

    cluster_to_potential_answerers = hour_cluster_to_potential_answerers[hour]

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
