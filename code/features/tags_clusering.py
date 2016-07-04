# -*- coding: utf-8 -*-
"""
Created on Sat Jul  2 12:22:00 2016

@author: neelima
"""
import boto
import pickle
import logging
import pyspark as ps
import pandas as pd
import numpy as np
from gensim.models import word2vec
from sklearn.cluster import KMeans

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
sq = SQLContext(sc)

def read_file(link):
    '''INPUT: file address
       OUTPUT: Pandas dataframe
    '''
    df = sq.read.format('com.databricks.spark.csv')\
            .option('header', 'true')\
            .option('mode', 'FAILFAST')\
            .option('delimiter', '|')\
            .option('inferSchema', True)\
            .load(link_tags).select('PostTypeId', 'Tags').cache()
    return df

def func_split(row):
    ''' INPUT: each row of a column
        OUTPU: returns a string of words seperated by ','
    '''
    return row.strip("><").split("><")

def data_preparation(dataframe= df_posts):
    '''INPUT: Pandas DataFrame
       OUTPUT: returns a list of list of Tags
           '''
    sq.registerDataFrameAsTable(df, "tags_table")
    df_tags = sq.sql('''SELECT * FROM tags_table WHERE PostTypeId = 1''')
    df_tags = df_tags.select('Tags').map(lambda row: func_split(row[0]))
    tags_of_questions = df_tags.collect()

def w2v_model(tags_of_questions):
    '''INPUT: List of lists of Tags
       OUTPUT: returns nXm numpy array
       where n = number of words,
       and, m = number of feature space
    '''
    word2vec_model = word2vec.Word2Vec(tags_of_questions, workers = 8, size = 300, min_count = 50, window = 20, sample = 0.0001)
    word2vec_model.save('/tmp/word2vec_model.bin')
    word_vectors = word2vec_model.syn0
    return word_vectors

def tag_clustering(word_vectors):
    '''INPUT: word_vectors array
       OUTPUT: Dictionary of tag to cluster number
    '''
    num_clusters = int((word_vectors.shape[0]/2)**0.5)
    kmeans_clustering = KMeans(n_clusters = num_clusters)
    idx = kmeans_clustering.fit_predict(word_vectors)

    tag_centroid_map = dict(zip( word2vec_model.index2word, idx )) # Mapping each tag to a cluster number
    return tag_centroid_map

def print_clusters(cluster = tag_centroid_map, num_clusters = len(tag_centroid_map.keys())):
    '''INPUT: dictionary of tag to cluster number
       OUTPUT: dictionary of cluster number to list of Tgas in that cluster
    '''
    clusters = []
    for cluster in xrange(0,num_clusters):
        words = []
        for i in xrange(0,len(word_centroid_map.values())):
            if( word_centroid_map.values()[i] == cluster ):
                words.append(word_centroid_map.keys()[i])
        clusters.append(set(words))
    return clusters

def visualize_cluster():
    '''
    '''
    pass

with open('/tmp/clusters.pickle', 'wb') as handle:
pickle.dump(clusters, handle)

if __name__ == '__main__':
    # get data from s3
    link = "s3n://project-neelima/posts.csv"
    df_posts = read_file(link)
    tags_list = data_preparation(df_posts)
    word_vectors = w2v_model(tags_list)
    tags_cluster = tag_clustering(word_vectors)
    # print clusters and list of tags
    print "Clusters and list of Tags in the cluster:"
    print "------------------------------------------"
    print print_clusters(tags_cluster)
