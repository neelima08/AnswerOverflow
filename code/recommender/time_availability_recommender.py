# coding: utf-8

# In[10]:

import pandas as pd
import numpy as np
import pickle
from datetime import datetime


# In[4]:

questions_answerers = pd.read_csv('/tmp/Question_Answerer.csv', delimiter='|')
questions_answerers['OwnerUserIdList'] = questions_answerers['OwnerUserId'].astype(list)
questions_answerers_list = questions_answerers.drop(['OwnerUserId'], axis=1)
questions_answerers_dict = questions_answerers_list.set_index('ParentId').to_dict()


# In[5]:

posts = pd.read_csv('/tmp/posts.csv', delimiter='|', usecols= ['Id', 'CreationDate', 'PostTypeId'])

questions = posts.loc[posts['PostTypeId'] == 1]
questions['CreationDate'] = questions['CreationDate'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f"))
questions_2016 = questions[(questions.CreationDate.dt.year >= 2016)]
questions_2016['Hour'] = questions_2016['CreationDate'].dt.hour

questions_hour_2016 = questions_2016.drop(['CreationDate', 'PostTypeId'], axis=1)
questions_hour_2016_dict = questions_hour_2016.set_index('Id').to_dict()


# In[35]:

with open('/tmp/hour_to_users.pickle', 'r') as handle:
    hour_to_users = pickle.load(handle)


# In[116]:

sampling = 1.0

hour_to_potential_answerers = {}
for hour in hour_to_users.keys():
    users = hour_to_users[hour]
    number_users = int(sampling*len(users))

    potential_answerers = set()
    for user_score in users[:number_users]:
        potential_answerers.add(user_score[0])

    hour_to_potential_answerers[hour] = potential_answerers


# In[118]:

total = 0
success = 0

for question in questions_hour_2016_dict['Hour'].keys():
    if question not in questions_answerers_dict['OwnerUserIdList']:
        continue

    total += 1
    hour = questions_hour_2016_dict['Hour'][question]
    potential_answerers = hour_to_potential_answerers[hour]

    answerers = set(map(int, questions_answerers_dict['OwnerUserIdList'][question].strip('[]').split(', ')))
    if len((potential_answerers).intersection(answerers)) > 0:
        success += 1

print (success, total)
