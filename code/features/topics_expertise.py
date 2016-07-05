import pandas as pd
import numpy as np
import pickle
import operator

def get_topic_to_tags_mapping(filename):
    with open(filename, 'rb') as handle:
        return pickle.load(handle)

def get_user_tag_expertise(filename):
    with open(filename, 'rb') as handle:
        return pickle.load(handle)

def get_tag_to_topic_mapping(topics_to_tags):
    """
    - Create a dict from tag to topic index
    """
    tags_to_topics = {}
    for topic_index in range(len(topics_to_tags)):
        for tag in topics_to_tags[topic_index]:
            tags_to_topics[tag] = topic_index

    return tags_to_topics

def get_user_topic_expertise_mapping(tags_to_topics, user_tag_expertise):
    """
    - Returns a dict of userId to dict of topic index to score
    """
    user_topic_expertise = {}
    
    for user in user_tag_expertise.keys():
        tags_expertise = user_tag_expertise[user]
        topic_expertise = {}
        for tag in tags_expertise.keys():
            if tag not in tags_to_topics:
                continue
        
            topic = tags_to_topics[tag]
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

def save_topics_expertise(topics_expertise, filename):
    with open(filename, 'w') as handle:
        pickle.dump(topics_expertise, handle)

if __name__ == '__main__':
    topics_to_tags = get_topic_to_tags_mapping('clusters.pickle')
    user_tag_expertise = get_user_tag_expertise('user_tag_expertise.pickle')

    tags_to_topics = get_tag_to_topic_mapping(topics_to_tags)
    user_topic_expertise = get_user_topic_expertise_mapping(tags_to_topics, user_tag_expertise)

    topics_expertise = get_topic_expert_users(user_topic_expertise)
    save_topics_expertise(topics_expertise, 'topics_expertise.pickle')
