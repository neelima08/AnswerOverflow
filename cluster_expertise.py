import pandas as pd
import numpy as np
import pickle

with open('/tmp/clusters.pickle', 'rb') as handle:
    clusters = pickle.load(handle)

with open('/tmp/user_tag_expertise.pickle', 'rb') as handle:
    user_tag_expertise = pickle.load(handle)

# Create a dict from tag to cluster index
tags_clusters = {}
for cluster_index in range(len(clusters)):
    for tag in clusters[cluster_index]:
        tags_clusters[tag] = cluster_index

# Calculate user expertise for a cluster. Dict of userId to dict of cluster index to score
user_cluster_expertise = {}
for user in user_tag_expertise.keys():
    tags_expertise = user_tag_expertise[user]
    cluster_expertise = {}
    for tag in tags_expertise.keys():
        if tag not in tags_clusters:
            continue
        
        cluster = tags_clusters[tag]
        cluster_expertise[cluster] = cluster_expertise.get(cluster, 0) + tags_expertise[tag]
        
    user_cluster_expertise[user] = cluster_expertise

# Create a dict of cluster index to list of tuples, where each tuple is userId and score
clusters_expertise = {}
for user in user_cluster_expertise.keys():
    clusters = user_cluster_expertise[user]
    for cluster in clusters.keys():
        score = clusters[cluster]
        
        cluster_expertise = clusters_expertise.get(cluster, [])
        cluster_expertise.append((user, score))
        clusters_expertise[cluster] = cluster_expertise

# Sort the list for each cluster index based on score
import operator

for cluster in clusters_expertise:
    cluster_expertise = clusters_expertise[cluster]
    cluster_expertise.sort(key=operator.itemgetter(1), reverse=True)
    clusters_expertise[cluster] = cluster_expertise
