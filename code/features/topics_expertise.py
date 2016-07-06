import pandas as pd
import numpy as np
import pickle
import operator

def load_from_pickle(filename):
    with open(filename, 'rb') as handle:
        return pickle.load(handle)

def save_as_pickle(object_to_save, filename):
    with open(filename, 'w') as handle:
        pickle.dump(object_to_save, handle)

def get_tag_to_topic_mapping(topics_to_tags):
    """
    - Create a dict from tag to topic index
    """
    tags_topic_mapping = {}
    for topic_index in range(len(topics_to_tags)):
        for tag in topics_to_tags[topic_index]:
            tags_topic_mapping[tag] = topic_index

    return tags_topic_mapping

def get_user_topic_expertise_mapping(tags_topic_mapping, user_tag_expertise):
    """
    - Returns a dict of userId to dict of topic index to score
    """
    user_topic_expertise = {}
    
    for user in user_tag_expertise.keys():
        tags_expertise = user_tag_expertise[user]
        topic_expertise = {}
        for tag in tags_expertise.keys():
            if tag not in tags_topic_mapping:
                continue
        
            topic = tags_topic_mapping[tag]
            topic_expertise[topic] = topic_expertise.get(topic, 0) + tags_expertise[tag]
        
        user_topic_expertise[user] = topic_expertise

    return user_topic_expertise

def get_topic_expert_users(user_topic_expertise):
    """
    - Returns a dict of topic index to list of users. This list is sorted by user expertise.
    """
    # Create a dict of topic index to list of tuples, where each tuple is userId and score
    topics_expertise = {}
    for user in user_topic_expertise.keys():
        topics = user_topic_expertise[user]
        for topic in topics.keys():
            score = topics[topic]
        
            topic_expertise = topics_expertise.get(topic, [])
            topic_expertise.append((user, score))
            topics_expertise[topic] = topic_expertise

    # Sort the list for each topic index based on score
    for topic in topics_expertise:
        topic_expertise = topics_expertise[topic]
        topic_expertise.sort(key=operator.itemgetter(1), reverse=True)
        topics_expertise[topic] = topic_expertise


if __name__ == '__main__':
    topics_to_tags = load_from_pickle('topics.pickle')
    user_tag_expertise = load_from_pickle('user_tag_expertise.pickle')

    tags_topic_mapping = get_tag_to_topic_mapping(topics_to_tags)
    user_topic_expertise = get_user_topic_expertise_mapping(tags_topic_mapping, user_tag_expertise)

    topics_expertise = get_topic_expert_users(user_topic_expertise)

    save_as_pickle(topics_expertise, 'topics_expertise.pickle')
    save_as_pickle(tags_topic_mapping, 'tags_topic.pickle')
