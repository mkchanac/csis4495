from asyncio.windows_events import NULL
import pandas as pd
import json
import gzip
# import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats


# data source from http://deepyeti.ucsd.edu/jianmo/amazon/index.html, need to submit google form to the author

def parse(path):
    g = gzip.open(path, 'rb')
    for l in g:
        yield json.loads(l)


def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')



def extractReviewsRawDataToJson(filename):
  
  df = getDF('D:\\Amazon\\review\\' + filename + '.json.gz')

  # clean the data, select the data we want
  # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html

  df = df[df.verified == True]
  df = df[
      df['reviewTime'].str.contains('2012')
      | df['reviewTime'].str.contains('2013')
      | df['reviewTime'].str.contains('2014')
      | df['reviewTime'].str.contains('2015')
      | df['reviewTime'].str.contains('2016')
      | df['reviewTime'].str.contains('2017')
      | df['reviewTime'].str.contains('2018')
  ]


  df = df.drop(columns=['reviewTime','verified','reviewerName','unixReviewTime',
              'vote', 'style', 'image', 'reviewText', 'summary'])
  print(df.head())

  df.to_json('C:\\Users\\unite\\Desktop\\test\\' + filename +'_product_reviews.json')


# extractReviewsRawDataToJson("Appliances")
# https://jsonformatter.org/json-viewer


def LoadAsDataFrameFromJsonFile(filename):
  df = pd.read_json('C:\\Users\\unite\\Desktop\\test\\'+ filename + '_product_reviews.json')
  print(df.head())
  return df 



# df2 = getDF('D:\\Amazon\\product\\meta_Appliances.json.gz')
# df2 = df2.drop(columns=['brand', 'similar_item', 'tech1', 'tech2', 'category', 'main_cat', 'feature',
#                'fit', 'details', 'rank', 'also_view', 'also_buy', 'date', 'description', 'imageURL'])

# # The better the sales rank (a lower number = better rank), the more sales it's getting on Amazon.

# # drop any Nan values, i.e cause image/price Nan = the product no longer in sales.
# df2 = df2.dropna()
# df2 = df2[df2['price'] != ""]
# df2 = df2[df2['imageURLHighRes'].str.len() != 0]
# # check if we can get the product photo url
# # print(df2.at[0,'imageURLHighRes'])

# # https://www.amazon.com/dp/[asin number]

# # print(df2.head())


# # df2.head().to_json('C:\\Users\\unite\\Desktop\\test\\file.json')

# df3 = pd.merge(df, df2, on='asin')

# print(df3.head())
# df3.head(100).to_json('C:\\Users\\unite\\Desktop\\test\\file2.json')


# # create a new table for only capturing average overall ratings as x-axis, num of reviews as y-axis
# avg_reviews = pd.DataFrame(df3.groupby('title')['overall'].mean())
# avg_reviews['num of reviews'] = pd.DataFrame(
#     df3.groupby('title')['overall'].count())
# # filter, we only recommend the product with at least 101 reviews.
# avg_reviews = avg_reviews[avg_reviews['num of reviews'] > 100]
# # avg_reviews = avg_reviews[avg_reviews['num of reviews']<1500]

# print(avg_reviews.head())
# avg_reviews.head().to_json('C:\\Users\\unite\\Desktop\\test\\file1.json')

# # json file, string replace using node.js https://appdividend.com/2022/02/14/javascript-remove-character-from-string/

# sns.set_style('white')

# # plot graph, alpha=transparency of the data dot
# jointplot = sns.jointplot(
#     x='overall', y='num of reviews', data=avg_reviews, alpha=0.5)
# # calculate the pearsonr correlation of the data plot
# print(stats.pearsonr(avg_reviews['overall'], avg_reviews['num of reviews']))
# plt.show()


# # take 1 to 3 categories passed by frontend MERN via nodejs spawn child process,
# # each category recommend few products, return the product asin and correlation score as json file to nodejs
# # https://www.w3schools.com/python/pandas/pandas_json.asp
