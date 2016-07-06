# Routing Questions to Potential Answerers

With rapid growth in Q&A websites like quora and reddit it has become important to route questions to a target set of users who are likely to answer. For it is important to recieve timely and good quality answers for good user engagement. In this project I have tried to address the same problem. For my project I am using StackOverflow data.

#Data
Collected data from www.archive.org . The data was in XML format. I wrote an XML to CSV converter script(can be found in install.sh). Used S3 to save the flies, and used SPARK for data processing.

#Features
The recommender mode has two features:
i.  User Topical Expertise 
ii. User Time Availability
      
      used Word2Vec, K-NN, t-sne and Sigmoid squashing

#Recommender 
Using the above two features I have build three recommenders:
i.  Recommending questions to those users who are available at the time question was asked.
ii. Recommending questions to the users who have the topical expertise required for the question
iii.Recommending questions to the users who have the topical expertise and are available at the time the question is asked.

#Future Work

This project can be expanded to include other features like user's compatibility score where we can route questions to those users who have collaborated on questions previously. It can then be used on streaming data to evaluate its performance in real-time.
