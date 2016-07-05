from gensim.models import Word2Vec
from tsne import bh_sne
import numpy,json,re
from matplotlib import pyplot as plt
get_ipython().magic(u'matplotlib inline')

word2vec_model = Word2Vec.load('word2vec_model.bin')

word_vectors = word2vec_model.syn0
word_vectors = word_vectors.astype('float64')

vis_data = bh_sne(word_vectors)

vis_x = vis_data[:, 0]
vis_y = vis_data[:, 1]

tags_to_plot = set()
for tag in word2vec_model.most_similar('c++', topn=100):
    tags_to_plot.add(tag[0])

for tag in word2vec_model.most_similar('javascript', topn=100):
    tags_to_plot.add(tag[0])
    
plt.figure(figsize=(20,20))
for i in range(0,len(vis_x)):
    tag = word2vec_model.index2word[i]
    if tag not in tags_to_plot:
        continue

    plt.scatter(vis_x[i], vis_y[i], s=len(tag)*1000, marker=r"$ {} $".format(tag))

plt.show()
