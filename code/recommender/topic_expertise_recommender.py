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


def xml2List(row):
    """
    - Converts XML elements to list. Example: <foo><bar> will become [foo, bar]
    INPUT: Elements in XML format
    OUTPUT: Elements in list format
    """
    return row.strip("><").split("><")


def get_question_tags_mapping(questions):
    """
    - Returns mapping from question to list of tags associated with that question
    """
    posts = pd.read_csv('posts.csv', delimiter='|', usecols= ['Id', 'PostTypeId', 'Tags'])

    questions = posts.loc[posts['PostTypeId'] == 1]
    questions['Tags'] = questions['Tags'].apply(xml2List)

    questions_tags = questions.drop(['PostTypeId'], axis=1)
    return questions_tags.set_index('Id').to_dict()


def load_from_pickle(filename):
    with open(filename, 'r') as handle:
        return pickle.load(handle)


def get_potential_answerers(topics_expertise):
    """
    - Returns set of potential answerers for each topic. This function takes a percentage of users from each hour.
    """
    sampling = 0.5

    topic_to_potential_answerers = {}
    for topic in topics_expertise.keys():
        users = topics_expertise[topic]
        number_users = int(sampling*len(users))

        potential_answerers = set()
        for user_score in users[:number_users]:
            potential_answerers.add(user_score[0])

        topic_to_potential_answerers[topic] = potential_answerers

    return topic_to_potential_answerers

def get_recommender_accuracy(questions_tags_dict, questions_answerers_dict, tags_topics_mapping, topic_to_potential_answerers):
    """
    - This function calculates and returns accuracy of prediction of an answerer for a question. This accuracy is calculated
      by matching whether answerer of a question appears in potential answerers set.
    """
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
    questions_tags_dict = get_question_tags_mapping()

    # Load user's topic expertise, i.e. topic to list of tuple where each tuple conists of UserID and score. This list is sorted wrt score.
    topics_expertise = load_from_pickle('topics_expertise.pickle')
    # Load mapping from tag to topicID
    tags_topics_mapping = load_from_pickle('tags_topic.pickle')

    topic_to_potential_answerers = get_potential_answerers(topics_expertise)
    recommender_accuracy = get_recommender_accuracy(questions_tags_dict, questions_answerers_dict, tags_topics_mapping, topic_to_potential_answerers)

    print("Accuracy of topic expertise recommender is %" % recommender_accuracy)
