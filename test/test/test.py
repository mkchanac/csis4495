import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# https://cseweb.ucsd.edu/~jmcauley/datasets.html#amazon_reviews
# https://nijianmo.github.io/amazon/index.html
# import json.gz

sns.set_style('white')


# pip3 install numpy

column_names = ['user_id', 'item_id', 'rating', 'timestamp']
df = pd.read_csv('u.data.csv',sep='\t',names=column_names)
print(df.head())

movie_titles = pd.read_csv("Movie_Id_Titles.csv")
print(movie_titles.head())

df = pd.merge(df, movie_titles, on='item_id')
print(df.head())


# Groupby one column and return the mean of only particular column in the group.

# df.groupby('A')['B'].mean()
# A
# 1    3.0
# 2    4.0
# Name: B, dtype: float64

# group by title and return the mean of rating
mean_rate_each_movie = df.groupby('title')['rating'].mean().sort_values(ascending=False).head()
print(mean_rate_each_movie)

total_no_of_review_each_movie = df.groupby('title')['rating'].count().sort_values(ascending=False).head()
print(total_no_of_review_each_movie)

# we will need to sort get most rated product from each .csv, send to front end, let them choose 
# 3 most interested product fit for their friends, then merge and sort and recommend!

ratings = pd.DataFrame(df.groupby('title')['rating'].mean())
# print(ratings.head())

ratings['num of ratings'] = pd.DataFrame(df.groupby('title')['rating'].count())
print(ratings.head())

# plot graph
jointplot = sns.jointplot(x='rating',y='num of ratings',data=ratings,alpha=0.5)
# calculate the pearsonr correlation of the data plot
print(stats.pearsonr(df.groupby('title')['rating'].mean(),df.groupby('title')['rating'].count()))
# plt.show()

# Generally, a value of r greater than 0.7 is considered a strong correlation. Anything between 0.5 and 0.7 is a moderate correlation, and anything less than 0.4 is considered a weak or no correlation.

moviemat = df.pivot_table(index='user_id',columns='title',values='rating')
print(moviemat.head())

print(ratings.sort_values('num of ratings',ascending=False).head(10))

starwars_user_ratings = moviemat['Star Wars (1977)']

print(starwars_user_ratings.head())

# recommend movie
similar_to_starwars = moviemat.corrwith(starwars_user_ratings)

# remove the na so we can proceed to recommend
corr_starwars = pd.DataFrame(similar_to_starwars,columns=['Correlation'])
corr_starwars.dropna(inplace=True)
print(corr_starwars.head())

# add one more column as num of ratings, set the filter, # of ratings > 100, so it make more senses
corr_starwars = corr_starwars.join(ratings['num of ratings'])
print(corr_starwars[corr_starwars['num of ratings']>100].sort_values('Correlation',ascending=False).head())

