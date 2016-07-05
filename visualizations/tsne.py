from gensim.models import Word2Vec
from tsne import bh_sne
import numpy
from matplotlib import pyplot as plt
get_ipython().magic(u'matplotlib inline')

def load_word2vec_model(filename):
	"""
	- Loads Word2Vec model from a file.
	"""
	return Word2Vec.load(filename)

def reduce_features_dim(word2vec_model):
	"""
	- Reduces features dimensionality using t-sne
	"""

	word_vectors = word2vec_model.syn0
	word_vectors = word_vectors.astype('float64')

	return bh_sne(word_vectors)

def plot_tags(vis_data, tags_to_plot):
	"""
	- Visualization of tags clustering using scatter plot
	"""

	vis_x = vis_data[:, 0]
	vis_y = vis_data[:, 1]

	plt.figure(figsize=(20,20))
	for i in range(0,len(vis_x)):
		tag = word2vec_model.index2word[i]
		if tag not in tags_to_plot:
			continue

		plt.scatter(vis_x[i], vis_y[i], s=len(tag)*1000, marker=r"$ {} $".format(tag))

	plt.show()

if __name__ == '__main__':
	word2vec_model = load_word2vec_model('word2vec_model.bin')
	vis_data = reduce_features_dim(word2vec_model)

	tags_to_plot = set()
	for tag in word2vec_model.most_similar('c++', topn=100):
		tags_to_plot.add(tag[0])

	for tag in word2vec_model.most_similar('javascript', topn=100):
		tags_to_plot.add(tag[0])

	plot_tags(vis_data, tags_to_plot)
