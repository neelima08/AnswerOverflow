import pandas as pd
import numpy as np
from gensim.models import word2vec
from sklearn.cluster import KMeans

df_posts = pd.read_csv('/tmp/posts.csv', delimiter='|')

def func_split(row):
    return row.strip("><").split("><")

tags_of_questions = df_posts.loc[df_posts['PostTypeId'] == 1]['Tags']
tags_of_questions = tags_of_questions.apply(func_split).tolist()


word2vec_model = word2vec.Word2Vec(tags_of_questions, workers = 8, size = 300, min_count = 50, window = 20, sample = 0.0001)
word2vec_model.save('/tmp/word2vec_model.bin')

word_vectors = word2vec_model.syn0
num_clusters = int((word_vectors.shape[0]/2)**0.5)

kmeans_clustering = KMeans(n_clusters = num_clusters)
idx = kmeans_clustering.fit_predict(word_vectors)

word_centroid_map = dict(zip( word2vec_model.index2word, idx ))

clusters = []
for cluster in xrange(0,num_clusters):
    # Find all of the words for that cluster number
    words = []
    for i in xrange(0,len(word_centroid_map.values())):
        if( word_centroid_map.values()[i] == cluster ):
            words.append(word_centroid_map.keys()[i])
    clusters.append(set(words))


import pickle
with open('/tmp/clusters.pickle', 'wb') as handle:
    pickle.dump(clusters, handle)



