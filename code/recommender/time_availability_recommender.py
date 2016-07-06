import pandas as pd
import numpy as np
import pickle
from datetime import datetime

def get_questions_answerers_mapping():
    """
    - Reads a csv file which contains mapping from question to list of anwerers and returns it as a dict
    """
    questions_answerers = pd.read_csv('Question_Answerer.csv', delimiter='|')
    questions_answerers['OwnerUserIdList'] = questions_answerers['OwnerUserId'].astype(list)
    questions_answerers_list = questions_answerers.drop(['OwnerUserId'], axis=1)

    return questions_answerers_list.set_index('ParentId').to_dict()

def get_questions_hour_mapping():
    """
    - Reads questions from posts file and return mapping from question to its created hour
    - This map consists of questions only from 2016. This is done as user availability feature
      is based on user's activity in 2016
    """
    posts = pd.read_csv('posts.csv', delimiter='|', usecols= ['Id', 'CreationDate', 'PostTypeId'])

    questions = posts.loc[posts['PostTypeId'] == 1]
    questions['CreationDate'] = questions['CreationDate'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f"))
    questions_2016 = questions[(questions.CreationDate.dt.year >= 2016)]
    questions_2016['Hour'] = questions_2016['CreationDate'].dt.hour

    questions_hour_2016 = questions_2016.drop(['CreationDate', 'PostTypeId'], axis=1)
    return questions_hour_2016.set_index('Id').to_dict()

def load_from_pickle(filename):
    with open(filename, 'r') as handle:
        return pickle.load(handle)

def get_potential_answerers(hour_to_users):
    """
    - Returns set of potential answerers for each hour. This function takes a percentage of users from each hour.
    """
    sampling = 0.5

    hour_to_potential_answerers = {}
    for hour in hour_to_users.keys():
        users = hour_to_users[hour]
        number_users = int(sampling*len(users))

        potential_answerers = set()
        for user_score in users[:number_users]:
            potential_answerers.add(user_score[0])

        hour_to_potential_answerers[hour] = potential_answerers

    return hour_to_potential_answerers

def get_recommender_accuracy(questions_answerers_dict, questions_answerers_dict, hour_to_potential_answerers):
    """
    - This function calculates and returns accuracy of prediction of an answerer for a question. This accuracy is calculated
      by matching whether answerer of a question appears in potential answerers set.
    """
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

    return (success * 100) / float(total)

if __name__ == '__main__':
    questions_answerers_dict = get_questions_answerers_mapping()
    questions_hour_2016_dict = get_questions_hour_mapping()

    # Load user avaiability feature, i.e. mapping from hours to list of users. This list is sorted by avaiability score.
    hour_to_users = load_from_pickle('hour_to_users.pickle')
    hour_to_potential_answerers = get_potential_answerers(hour_to_users)

    recommender_accuracy = get_recommender_accuracy(questions_hour_2016_dict, questions_answerers_dict, hour_to_potential_answerers)

    print("Time availability recommender is %" % recommender_accuracy)
