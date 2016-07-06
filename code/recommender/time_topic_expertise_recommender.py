import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import operator

def load_from_pickle(filename):
    with open(filename, 'r') as handle:
        return pickle.load(handle)

def get_questions_answerers_mapping():
    """
    - Reads a csv file which contains mapping from question to list of anwerers and returns it as a dict
    """
    questions_answerers = pd.read_csv('Question_Answerer.csv', delimiter='|')
    questions_answerers['OwnerUserIdList'] = questions_answerers['OwnerUserId'].astype(list)
    questions_answerers_list = questions_answerers.drop(['OwnerUserId'], axis=1)

    return questions_answerers_list.set_index('ParentId').to_dict()


def xml2List(row):
    """
    - Converts XML elements to list. Example: <foo><bar> will become [foo, bar]
    INPUT: Elements in XML format
    OUTPUT: Elements in list format
    """
    return row.strip("><").split("><")


def get_questions_hour_tag_mapping():
    """
    - Gets questions from posts file. Each row of question contains hour of question creation and list of tags associated with the question
    """
    posts = pd.read_csv('posts.csv', delimiter='|', usecols= ['Id', 'CreationDate', 'PostTypeId', 'Tags'])

    questions = posts.loc[posts['PostTypeId'] == 1]
    questions['CreationDate'] = questions['CreationDate'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f"))

    questions_2016 = questions[(questions.CreationDate.dt.year >= 2016)]
    questions_2016['Hour'] = questions_2016['CreationDate'].dt.hour
    questions_2016['Tags'] = questions_2016['Tags'].apply(xml2List)

    return questions_2016


def get_topic_user_expertise_mapping(topics_expertise):
    """
    - Returns a dictionary which maps topic to UserID and topic expertise score.
    """
    topics_expertise_dict = {}
    for topic in topics_expertise.keys():
        users_score_list = topics_expertise[topic]

        users_score_dict = {}
        for user_score_tuple in users_score_list:
            users_score_dict[user_score_tuple[0]] = user_score_tuple[1]

        topics_expertise_dict[topic] = users_score_dict

    return topics_expertise_dict


def get_hour_user_avaiability_mapping(user_availability):
    """
    - Returns a dictionary which maps hour to userID and availability score.
    """
    hour_to_users_availability = {}
    for hour in user_availability.keys():
        user_availability_list = user_availability[hour]

        user_availability_score_dict = {}
        for user_score_tuple in user_availability_list:
            user_availability_score_dict[user_score_tuple[0]] = user_score_tuple[1]
        
        hour_to_users_availability[hour] = user_availability_score_dict

    return hour_to_users_availability


def get_hour_user_expertise_mapping(hour_to_users_availability, topics_expertise_dict):
    """
    - Returns a dictionary which maps hour to topic, userID and score. Score is equal to (topic expertise + avaiability)
    """
    hour_user_expertise_dict = {}

    for hour in hour_to_users_availability.keys():
        user_availability_score_dict = hour_to_users_availability[hour]
        hour_users = set(user_availability_score_dict.keys())

        topic_expertise_dict_per_hour = {}
        for topic in topics_expertise_dict.keys():
            topic_users_score = topics_expertise_dict[topic]
            topic_users = set(topic_users_score.keys())

            users_topic_hour_expertise_list = []

            hour_topic_users = topic_users.union(hour_users)
            for user in hour_topic_users:
                users_topic_hour_expertise = user_availability_score_dict.get(user, 0) + topic_users_score.get(user, 0)
                users_topic_hour_expertise_list.append((user, users_topical_hour_expertise))

            topic_expertise_dict_per_hour[topic] = users_topic_hour_expertise_list

        hour_user_expertise_dict[hour] = topic_expertise_dict_per_hour

    for hour in hour_user_expertise_dict.keys():
        hour_user_expertise = hour_user_expertise_dict[hour]

        for topic in hour_user_expertise.keys():
            hour_user_expertise[topic].sort(key=operator.itemgetter(1), reverse=True)

    return hour_user_expertise_dict


def get_potential_answerers(hour_user_expertise_dict):
    """
    - Returns set of potential answerers for each topic and hour.
    """
    sampling = 0.5

    hour_topic_to_potential_answerers = {}
    for hour in hour_user_expertise_dict.keys():
        hour_user_expertise = hour_user_expertise_dict[hour]

        topic_potential_answerers = {}
        for topic in hour_user_expertise.keys():
            users = hour_user_expertise[topic]
            number_users = int(sampling*len(users))

            potential_answerers = set()
            for user_score in users[:number_users]:
                potential_answerers.add(user_score[0])

            topic_potential_answerers[topic] = potential_answerers

        hour_topic_to_potential_answerers[hour] = topic_potential_answerers

    return hour_topic_to_potential_answerers


def get_recommender_accuracy(questions_2016, questions_answerers_dict, tags_topics_mapping, hour_topic_to_potential_answerers):
    """
    - This function calculates and returns accuracy of prediction of an answerer for a question. This accuracy is calculated
      by matching whether answerer of a question appears in potential answerers set.
    """
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

        topic_to_potential_answerers = hour_topic_to_potential_answerers[hour]

        found = False
        for tag in tags:
            if tag not in tags_topics_mapping:
              continue

            topic = tags_topics_mapping[tag]
            potential_answerers = topic_to_potential_answerers[topic]
            if len((potential_answerers).intersection(answerers)) > 0:
                found = True
                break
        if found:
            success += 1

    return (success * 100) / float(total)

if __name__ == '__main__':
    questions_answerers_dict = get_questions_answerers_mapping()
    questions_2016 = get_questions_hour_tag_mapping()

    user_availability = load_from_pickle('hour_to_users.pickle')
    topics_expertise = load_from_pickle('topics_expertise.pickle')
    tags_topics_mapping = load_from_pickle('tags_topics.pickle')

    topics_expertise_dict = get_topic_user_expertise_mapping(topics_expertise)
    hour_to_users_availability = get_hour_user_avaiability_mapping(user_availability)
    hour_user_expertise_dict = get_hour_user_expertise_mapping(hour_to_users_availability, topics_expertise_dict)

    potential_answerers = get_potential_answerers(hour_user_expertise_dict)
    recommender_accuracy = get_recommender_accuracy(questions_2016, questions_answerers_dict, tags_topics_mapping, potential_answerers)

    print("Accuracy of topic expertise and user availability recommender is %s" % recommender_accuracy)
