# Routing Questions to Potential Answerers

With rapid growth in Q&A websites like quora and reddit, it has become important to route questions to potential answerers. This is needed to recieve timely and good quality answers for good user engagement. In this project, I am using StackOverflow data set to address this problem.

#Data
StackOverflow data was from www.archive.org. This data was in XML format and I wrote an XML to CSV converter (scripts/xml2csv.py) to convert it into CSV format. This dataset consisted of 11M questions, 18M answers, 1.3M users, 48.3M comments and 44K tags. After cleaning and processing data with Apache Spark, data was stored in S3.

This data was split in 70/30 training and test data, where 70% of the data was used to calculate features and train model and 30% of data was used to test recommender models.

#Features
Following features were calculated to build recommender model:
* **User Topical Expertise** - Tags were clustered into 97 topics using Word2Vec and KMeans. Visualizations of this clustering can be seen in visualizations folder where 300 feature dimensions were reduced to 2D using t-sne. User's topical expertise was calculated based on scores received on their answers for questions associated with particular tags. Special care was taken to ensure those users aren't classified as "experts" who answered few questions and received high score on them. This was addressed by calculating sigmoid of user's score.
* **User Time Availability** - User's availability is defined as user's activity in terms of comments and answers. This was calculated on an hour basis.

Code to calculate features is present in code/features directory.

#Recommender Model
Above features were used to build following recommender models. Accuracy of a recommender model was calculated by selecting a question from test data set and calculating potential answerers for that question. If any answerer from this recommendation set matched with actual answerers of a question, then score of 1 was provided to that question. Otherwise, that question was scored with a score of 0. In the end, all scores were added up to calculate model accuracy.

* **Based on user's availability** - This recommender model used only user's availability as a feature. This model provided 10.03% accuracy.
* **Based on user's topical expertise** - In this model, user's topic expertise was used which provided an accuracy of 63% accuracy.
* **Based on user's avaiability and topical expertise** - Logistic regression model based on user's topical expertise and availability were used in this model which provided an accuracy of 71%

#Future Work
* This project can be expanded to include other features like user's compatibility score where we can route questions to those users who have collaborated on questions previously.
* It can also be used on streaming data to evaluate its performance in real-time.
* Performance of these features and models can also be evaluated on other collaboration websites like Quora, Reddit etc.
