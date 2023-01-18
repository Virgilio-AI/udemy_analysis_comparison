# %%
!pip install gender_guesser

# %%
# Date: 21/December/2022 - Wednesday
# Author: Virgilio Murillo Ochoa
# personal github: Virgilio-AI
# linkedin: https://www.linkedin.com/in/virgilio-murillo-ochoa-b29b59203
# contact: data_scientist@virgiliomurillo.com
# web: virgiliomurillo.com

# %%
import pandas as pd
import time
import datetime

# %%
prev = datetime.datetime.now()
description = pd.read_csv("Course_info.csv", index_col = 0)
now = datetime.datetime.now()
print(now - prev)

# %%
prev = datetime.datetime.now()
comments = pd.read_csv("Comments.csv")
now = datetime.datetime.now()
print(now - prev)

# %%
# change the dataframe data type
prev = datetime.datetime.now()
description['num_subscribers'] = description['num_subscribers'].astype('int64')
description['num_reviews'] = description['num_reviews'].astype('int64')
description['num_comments'] = description['num_comments'].astype('int64')
description['num_lectures'] = description['num_lectures'].astype('int64')
description['num_reviews'] = description['num_reviews'].astype('int64')
description['published_time'] = pd.to_datetime(description['published_time'])
description['last_update_date'] = pd.to_datetime(description['last_update_date'])


# get the username from instructor url
description['instructor_url'] = description['instructor_url'].str[6:-2]

# get the name of the course
description['course_url'] = description['course_url'].str[8:-1]

# rename the column named 'instructor_url' to 'instructor_username'
description.rename(columns = {'instructor_url':'instructor_username'}, inplace = True)


now = datetime.datetime.now()
print(now - prev)

# %%
description.info()

# %%
description.describe()

# %%
description.head()

# %%
english_dataframe = description[description['language'] == 'English' ] 

# %%
english_dataframe.info()

# %%
instructors_df = english_dataframe.groupby(['instructor_username','instructor_name']).agg({'num_subscribers':'mean','avg_rating':'mean', 'price':'mean'})

# %%
instructors_df.head()

# %%
instructors_df.reset_index(inplace = True)

# %%
instructors_df.head()

# %%
instructors_df = instructors_df.rename(columns={'instructor_username':'username','instructor_name':'name','num_subscribers':'avg_num_subscribers','price':'avg_price'})

# %%
instructors_df.head()

# %%
instructors_df = instructors_df.set_index('username')

# %%
instructors_df.head()


# %%
def getFirstName(name):
	if name is None:
		return "None"
	sname = name.split()
	prefix = ['mr.','mrs.','ms.','dr.','prof.','sr.','jr.','.',',','mr','mrs','ms','dr','prof','sr','jr']
	for i in range(len(sname)):
		sname[i] = sname[i].lower()


	if len(sname) > 0 and sname[0] not in prefix:
		return sname[0]
	elif len(sname) > 1 and sname[1] not in prefix:
		return sname[1]
	elif len(sname) > 2 and sname[2] not in prefix:
		return sname[2]

# %%
instructors_df['name']= instructors_df['name'].map(getFirstName)

# %%
instructors_df.head()

# %%
import gender_guesser.detector as gender
gd = gender.Detector()

# %%
instructors_df['gender'] = instructors_df['name'].map(lambda x: gd.get_gender(x.capitalize()))

# %%
instructors_df.head()

# %%
unk_instructors = instructors_df[instructors_df['gender'] == 'unknown']

# %%
unk_instructors.describe()

# %%
prev = datetime.datetime.now()
# get all the unique names
gbyname = unk_instructors.groupby('name').agg({'name':'count'})
gbyname.rename(columns={'name':'count'},inplace=True)
gbyname.describe()
now = datetime.datetime.now()
print(now - prev)

# %%
gbyname = gbyname.sort_values(by='count',ascending=False)

# %%
# se tendria que hacer una labor de identificarlos a mano o encontrar un diccionario en linea que contenga nombres de indios
gbyname.head()

# %%
known_instructors = instructors_df[instructors_df['gender'] != 'unknown']

# %%
known_instructors.describe()

# %%
known_instructors.head()

# %%

# now just make graphs to see what are the most successfull intructors according to the table

# %%
import matplotlib.pyplot as plt
import seaborn as sns

# %%
# the most succesfull according to sex
avg_num_subscribers_per_gender = known_instructors.groupby('gender').agg({'avg_num_subscribers':'mean'})

# %%
avg_num_subscribers_per_gender.head()

# %%
sns.barplot(x = avg_num_subscribers_per_gender.index, y = 'avg_num_subscribers', data = avg_num_subscribers_per_gender)

# %%
avg_rating_per_gender = known_instructors.groupby('gender').agg({'avg_rating':'mean'})

# %%
avg_rating_per_gender

# %%
# set the x range to be from 3 to 5
plt.ylim(3,5)
sns.barplot(x = avg_rating_per_gender.index, y = 'avg_rating', data = avg_rating_per_gender)
