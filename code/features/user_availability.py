import pandas as pd
from datetime import datetime
import collections
from collections import Counter
import pickle
import numpy as np
import operator

def get_answers():
	'''
	- Reads answers from posts file. Answers data frame has these columns: 1) UserID, 2) Time of answer
	'''
	posts = pd.read_csv('posts.csv', delimiter = '|', usecols= ['PostTypeId','OwnerUserId', 'LastActivityDate'])
	answers = posts[posts['PostTypeId'] == 2]
	answers.drop('PostTypeId' , axis = 1, inplace = True)
	answers.columns = ['UserId', 'Time']
	answers.dropna(inplace = True)
	answers['UserId'] = answers['UserId'].astype(int)
	answers['Time'] = answers['Time'].astype(str)

def format_time(answers):
	'''
	- Formats time column in a datetime format
	'''
	answers['Time'] = answers['Time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f"))
	return answers

def get_active_users(answers):
	'''
	- Gets active users. Active users are ones which have answered in 2016
	'''
	active_users = answers[(answers.Time.dt.year >= 2016)]
	active_users['Hour'] = active_users['Time'].dt.hour

	return active_users

def get_user_hour_mapping(active_users):
	'''
	- Create a dataframe which consists of UserID and list of hours in which questions were answered
	'''
	users_answer_per_hour = active_users.groupby('UserId')['Hour'].apply(list).reset_index()

	# Keep only those users which have answered more than 5 questions 
	users_answer_per_hour['Hour'] = users_answer_per_hour['Hour'].apply(lambda x: x if len(x) > 5 else np.nan)

	users_answer_per_hour.dropna(inplace=True)
	return users_answer_per_hour

def calculate_availability_score(lst):
	'''
	- Calculates availability score of a user. Availability score is defined as (number of questions answered in an hour / total number of answers)
	'''
    total_questions = len(lst)
    hour_question = Counter(lst)
    for key, value in hour_question.items():
        hour_question[key] = value/float(total_questions)
    return hour_question

def get_user_avaiability_score(users_answer_per_hour):
	'''
	- Calculates availability score of all users. Returned data frame has 2 columns: 1) UserID, 2) Dictionary of hour to availability score
	'''
	users_answer_per_hour['AvailScore'] = users_answer_per_hour['Hour'].apply(calculate_availability_score)

	return users_answer_per_hour

def get_hour_to_availability_score:
	'''
	- Returns a dictionary from hour to list of tuples, where each tuple consists of UserID and availability score. This list is sorted by availability score.
	'''
	hour_to_availability_score_cict = dict([(user, hour) for user, hour in zip(users_answer_per_hour.UserId, users_answer_per_hour.AvailScore)])

	hour_to_users = {}
	for user in d.keys():
	    per_hour_answers = d[user]
	    for hour in per_hour_answers.keys():
	        users = hour_to_users.get(hour, [])
	        
	        users.append((user, per_hour_answers[hour]))
	        hour_to_users[hour] = users

	for hour in hour_to_users.keys():
	    user_scores = hour_to_users[hour]
	    user_scores.sort(key=operator.itemgetter(1), reverse=True)
	    hour_to_users[hour] = user_scores

	return hour_to_users

def save_as_pickle(object_to_save, filename):
    with open(filename, 'w') as handle:
        pickle.dump(object_to_save, handle)

if __name__ == '__main__':
	answers = get_answers()
	answers_formatted_time = format_time(answers)

	active_users = get_active_users(answers_formatted_time)
	users_answer_per_hour = get_user_hour_mapping(active_users)

	users_answer_per_hour = get_user_avaiability_score(users_answer_per_hour)
	hour_to_users = get_hour_to_availability_score(users_answer_per_hour)

	save_as_pickle(hour_to_users, 'hour_to_users.pickle')
