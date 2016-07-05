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

def read_file(s3_url):
    """
    - Reads CSV file from S3 in pandas data frame
    - INPUT: S3 file URN
    - OUTPUT: Pandas dataframe
    """
    return sq.read.format('com.databricks.spark.csv')\
            .option('header', 'true')\
            .option('mode', 'FAILFAST')\
            .option('delimiter', '|')\
            .option('inferSchema', True)\
            .load(s3_url).select('PostTypeId', 'Tags').cache()

def xml2List(row):
    """
    - Converts XML elements to list. Example: <foo><bar> will become [foo, bar]
    INPUT: Elements in XML format
    OUTPUT: Elements in list format
    """

    return row.strip("><").split("><")

def get_questions(posts):
    """
    - Returns questions from posts data frame
    """

    sq.registerDataFrameAsTable(df, "posts_table")
    return sq.sql('''SELECT * FROM tags_table WHERE PostTypeId = 1''')

def get_tags(questions):
    """
    - Returns list of tags associated with each question
    - INPUT: Questions
    - OUTPUT: List of list of Tags
    """

    df_tags = questions.select('Tags').map(lambda row: xml2List(row[0]))
    return df_tags.collect()

def init_w2v_model(tags_list):
    """
    - Creates word2vec model
    - INPUT: List of lists of Tags
    - OUTPUT: returns nXm numpy array, where n = number of words; m = number of dimensions of feature space
    """

    return word2vec.Word2Vec(tags_of_questions, workers = 8, size = 300, min_count = 50, window = 20, sample = 0.0001)

def save_w2v_model(word2vec_model, filename):
    """
    - Saves word2vec model
    """

    word2vec_model.save('/tmp/word2vec_model.bin')

def create_tag_clusters(word2vec_model):
    """
    - Create clusters of tags using KMeans
    - INPUT: Word2Vec model
    - OUTPUT: Dictionary of tag to cluster number
    """

    word_vectors = word2vec_model.syn0
    num_clusters = int((word_vectors.shape[0]/2)**0.5)
    kmeans_clustering = KMeans(n_clusters = num_clusters)
    idx = kmeans_clustering.fit_predict(word_vectors)

    return dict(zip( word2vec_model.index2word, idx )) # Mapping each tag to a cluster number


def cluster_to_tags_mapping(tag_to_cluster_map):
    """
    - Creates a map from cluster to list of tags
    - INPUT: dictionary of tag to cluster number
    - OUTPUT: dictionary of cluster number to list of tags in that cluster
    """

    num_clusters = len(tag_to_cluster_map.keys())
    clusters_to_tags = []

    for cluster in xrange(0,num_clusters):
        words = []
        for i in xrange(0,len(tag_to_cluster_map.values())):
            if( tag_to_cluster_map.values()[i] == cluster ):
                words.append(tag_to_cluster_map.keys()[i])
        clusters_to_tags.append(set(words))
    
    return clusters_to_tags

def save_cluster_to_tags_mapping(clusters_to_tags, filename):
    with open(filename, 'w') as handle:
        pickle.dump(clusters_to_tags, handle)


if __name__ == '__main__':
    # get data from s3
    s3_url = "s3n://project-neelima/posts.csv"
    posts = read_file(s3_url)
    questions = get_questions(posts)

    tags = get_tags(questions)
    word2vec_model = init_w2v_model(tags)
    save_w2v_model(word2vec_model, 'word2vec_model.bin')

    tag_to_cluster_map = create_tag_clusters(word_vectors)
    cluster_to_tag_map = cluster_to_tags_mapping(tag_to_cluster_map)
    save_cluster_to_tags_mapping(cluster_to_tag_map)
