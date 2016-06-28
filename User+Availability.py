
# coding: utf-8

# In[4]:

import pandas as pd
from datetime import datetime
import collections
from collections import Counter


# In[5]:

df_answers = pd.read_csv('/tmp/posts.csv', delimiter = '|', usecols= ['PostTypeId','OwnerUserId', 'LastActivityDate'])


# In[8]:

df_answers = df_answers[df_answers['PostTypeId'] == 2]


# In[10]:

df_answers.drop('PostTypeId' , axis = 1, inplace = True)


# In[11]:

df_answers.columns = ['UserId', 'Time']


# In[13]:

df_answers.dropna(inplace = True)
df_answers['UserId'] = df_answers['UserId'].astype(int)
df_answers['Time'] = df_answers['Time'].astype(str)


# In[7]:

df_comments = pd.read_csv('/tmp/comments.csv', delimiter = '|', usecols= ['UserId','CreationDate'])


# In[16]:

df_comments.columns = ['Time', 'UserId']


# In[18]:

df_comments.dropna(inplace = True)
df_comments['UserId'] = df_comments['UserId'].astype(int)
df_comments['Time'] = df_comments['Time'].astype(str)


# #  Merging Answers and Comments Table

# In[19]:

df = df_answers.append(df_comments, ignore_index = True)


# # Transformations

# In[22]:

df_answers['Time'] = df_answers['Time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f"))


# In[23]:

print "Starting Date:", df_answers['Time'].min()
print "Last Date:", df_answers['Time'].max()


# In[24]:

df_active_users = df_answers[(df_answers.Time.dt.year >= 2016)]
#df_active_users = df[(df.Time.dt.year >= 2015) & (df.Time.dt.month > 1)]


# In[25]:

df_active_users.count()


# In[26]:

df_active_users['Hour'] = df_active_users['Time'].dt.hour


# In[40]:

users_answer_per_hour = df_active_users.groupby('UserId')['Hour'].apply(list).reset_index()


# In[42]:

def dict_question_count(lst):
    total_questions = len(lst)
    hour_question = Counter(lst)
    for key, value in hour_question.items():
        hour_question[key] = value/float(total_questions)
    return hour_question


# In[43]:

users_answer_per_hour['Hour'] = users_answer_per_hour['Hour'].apply(dict_question_count)


# In[44]:

users_answer_per_hour.head()


# In[45]:

import pickle
with open('/tmp/users_answer_per_hour', 'wb') as handle:
    pickle.dump(users_answer_per_hour, handle)


# In[ ]:




# In[ ]:



