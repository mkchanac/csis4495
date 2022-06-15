from asyncio.windows_events import NULL
import pandas as pd
import json
import gzip
import numpy as np
import os   
import os.path
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats


# data source from http://deepyeti.ucsd.edu/jianmo/amazon/index.html, need to submit google form to the author
# method from the data source provider: Jianmo
def parse(path):
    with gzip.open(path, 'rb') as g:
      for l in g:
        yield json.loads(l)
# method from the data source provider: Jianmo
def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')


# a function to take review raw data .json.gz,
# extract only the required data from 2 dataset product_detail And product_review, 
# merge 2 dataset into 1 dataset, and output as product_detail_review.csv file
# so we could use it later by panda.read_csv
def processDataToCSV(category):
  
  # Process the product_review dataset to panda.DataFrame
  product_review_df = getDF('D:\\Amazon\\review\\' + category + '.json.gz')
  # clean the data, select the data we want
  # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html
  product_review_df = product_review_df[product_review_df.verified == True]
    # drop unneccessary data
  product_review_df = product_review_df.drop(columns=['reviewTime','verified','reviewerName','unixReviewTime',
              'vote', 'style', 'image', 'reviewText', 'summary'])
  # extract only 5 years reviews
  product_review_df = product_review_df[
        product_review_df['reviewTime'].str.contains('2014')
      # | product_review_df['reviewTime'].str.contains('2015')
      # | product_review_df['reviewTime'].str.contains('2016')
      # | product_review_df['reviewTime'].str.contains('2017')
      # | product_review_df['reviewTime'].str.contains('2018')
  ]

  print(category + ': Product Review Data from 2014-2018')
  print(product_review_df.head())

  # Process the product_detail dataset to panda.DataFrame
  product_detail_df = getDF('D:\\Amazon\\product\\meta_'+ category + '.json.gz')
  # clean the data, select the data we want, drop the unnnecessary data
  product_detail_df = product_detail_df.drop(columns=['brand', 'similar_item', 'tech1', 'tech2', 'category', 'main_cat', 'feature',
               'fit', 'details', 'rank', 'also_view', 'also_buy', 'date', 'description', 'imageURL'])
  # drop any Nan values, i.e cause image/price Nan = the product no longer in sales.
  product_detail_df = product_detail_df.dropna()
  product_detail_df = product_detail_df[product_detail_df['price'] != ""]
  product_detail_df = product_detail_df[~(product_detail_df['price'].str.contains(";"))]
  product_detail_df = product_detail_df[product_detail_df['imageURLHighRes'].str.len() != 0]  # filter the empty imageURL list
  # check if we can get the product photo url
  # print(df2.at[0,'imageURLHighRes'])
  # https://www.amazon.com/dp/{asin}
  print(category + ': Product Detail Data')
  print(product_detail_df.head())

  # Merge the product_detail and product_review together, product_detail_review.csv
  product_detail_review_df = pd.merge(product_detail_df, product_review_df, on='asin')
  print(category + ': Product Detail And Product Review Data')
  print(product_detail_review_df.head())
  os.makedirs('D:\\Amazon\\processed_data', exist_ok=True)  
  product_detail_review_df.to_csv('D:\\Amazon\\processed_data\\' + category + '_product_detail_review.csv') 

# processDataToCSV('All_Beauty')

# we might get json file from nodejs in future
def loadAsDataFrameFromJsonFile(filename):
  df = pd.read_json('C:\\Users\\unite\\Desktop\\test\\'+ filename)
  print(df.head())
  return df 

# we might get csv file from nodejs in future
def loadAsDataFrameFromCSVFile(filename):
  df = pd.read_csv('D:\\Amazon\\processed_data\\' + filename + '.csv')
  print(df.head())
  return df 


# Groupby one column and return the mean of only particular column in the group.

# df.groupby('A')['B'].mean()
# A
# 1    3.0
# 2    4.0
# Name: B, dtype: float64
def computeAvgRatingsOfProductAndPlotGraph(category, product_detail_review_df):

  # create a new table for only capturing average overall ratings as x-axis, num of reviews as y-axis
  avg_reviews = pd.DataFrame(product_detail_review_df.groupby('asin')['overall'].mean())
  avg_reviews['num_of_reviews'] = pd.DataFrame(product_detail_review_df.groupby('asin')['overall'].count())
  print(category + ': Product Average Review Data')
  print(avg_reviews.head())
  # filter
  # avg_reviews = avg_reviews[avg_reviews['num of reviews'] > 100]
  # avg_reviews = avg_reviews[avg_reviews['num of reviews'] < 1500]
  avg_reviews.to_csv('D:\\Amazon\\processed_data\\' + category + '_product_avg_reviews.csv') 
  # json file, string replace using node.js https://appdividend.com/2022/02/14/javascript-remove-character-from-string/
  sns.set_style('white')
  # plot graph, alpha=transparency of the data dot
  sns.jointplot(x='overall', y='num_of_reviews', data=avg_reviews, alpha=0.5)
  # calculate the pearsonr correlation of the data plot
  print(stats.pearsonr(avg_reviews['overall'], avg_reviews['num_of_reviews']))
  plt.show()



# # take 1 to 3 categories passed by frontend MERN via nodejs spawn child process,
# # each category recommend few products, return the product asin sort by correlation score as json file to nodejs
# # https://www.w3schools.com/python/pandas/pandas_json.asp
# # create a pivot table, so we can find out the user with similar taste, and recommend the products by computing correlation 

def recommendByCategory(category):


  # Run the processDataToCSV to generate a new XXXXXX_product_detail_review.csv
  if(os.path.exists('D:/Amazon/processed_data/' + category + '_product_detail_review.csv')):
    print('product detail review exists')
  else:
    processDataToCSV(category)
    
  product_detail_review_df = loadAsDataFrameFromCSVFile(category +'_product_detail_review')

  # Run the computeAvgRatingsOfProductAndPlotGraph to generate a new XXXXXX_product_avg_reviews.csv
  if(os.path.exists('D:/Amazon/processed_data/' + category + '_product_avg_reviews.csv')):
    print('product avg review exists')
  else:
    computeAvgRatingsOfProductAndPlotGraph(category, product_detail_review_df)

  # just get the top 10 highest no. of ratings product
  avg_reviews_df = loadAsDataFrameFromCSVFile(category + '_product_avg_reviews')
  # add a new column 'total' = 'overall' * 'num of reviews' for sorting
  avg_reviews_df['total'] = avg_reviews_df.overall * avg_reviews_df.num_of_reviews

  # get only top 5 products's asin, make the column object becomes string
  asin_array = avg_reviews_df.sort_values('total',ascending=False).head()['asin']   
  print(asin_array)

  recommend_products = []
  product_matrix = product_detail_review_df.pivot_table(index='reviewerID',columns='asin',values='overall')
  print(product_matrix.head())

  for asin in asin_array:
    data = recommendByAsin(asin, product_matrix, avg_reviews_df)   # a function created by us
    recommend_products.append(data)
  # panda tut: https://datacarpentry.org/python-socialsci/11-joins/index.html
  recommend_products = pd.concat(recommend_products)
  # only recommend the product with the correlation score > 0.5
  recommend_products = recommend_products[recommend_products['Correlation'] > 0.5]
  recommend_products = recommend_products.sort_values('Correlation',ascending=False).head(20)
  print(recommend_products)
  recommend_products.to_csv('D:\\Amazon\\processed_data\\' + category + '_recommend_products.csv') 

def recommendByAsin(asin, product_matrix, avg_reviews_df):
  asin_user_ratings = product_matrix[asin]
  print(asin_user_ratings.head())
  similar_to_asin = product_matrix.corrwith(asin_user_ratings)
  # remove the na so we can proceed to recommend
  corr_asin = pd.DataFrame(similar_to_asin,columns=['Correlation'])
  corr_asin.dropna(inplace=True)
  print(corr_asin.head())
  # merge them on asin
  corr_asin = pd.merge(corr_asin, avg_reviews_df, on='asin')
  # recommend at most 20 products
  print(corr_asin.sort_values('Correlation',ascending=False).head(20))
  return (corr_asin.sort_values('Correlation',ascending=False).head(20))



# recommendByCategory('Arts_Crafts_and_Sewing')
# # our recommendation will mix all the categories XXXX_recommend_products.csv and sort them by correlation score
# def recommend_from_nodejs(categories_array):
  # get json file from nodejs later, hoping the MERN set up will be done before midterm
  # loadAsDataFrameFromJsonFile(jsonfile)



