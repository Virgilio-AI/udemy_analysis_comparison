# %%
# Date: 21/December/2022 - Wednesday
# Author: Virgilio Murillo Ochoa
# personal github: Virgilio-AI
# linkedin: https://www.linkedin.com/in/virgilio-murillo-ochoa-b29b59203
# contact: data_scientist@virgiliomurillo.com
# web: virgiliomurillo.com

# %%
# import libraries

# %%
import pandas as pd
import time
import datetime
from numpy.random import randint
import numpy as np # for importing numpy
import matplotlib.pyplot as plt # for importing matplotlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, udf,regexp_extract

# %% language="javascript"
# (function(on) {
#     const e = $("<a>Setup failed</a>");
#     const ns = "js_jupyter_suppress_warnings";
#     var cssrules = $("#" + ns);
#     if(!cssrules.length)
#         cssrules = $("<style id='" + ns + "' type='text/css'>div.output_stderr { } </style>").appendTo("head");
#     e.click(function() {
#         var s = 'Showing';
#         cssrules.empty()
#         if(on) {
#             s = 'Hiding';
#             cssrules.append("div.output_stderr, div[data-mime-type*='.stderr'] { display:none; }");
#         }
#         e.text(s + ' warnings (click to toggle)');
#         on = !on;
#     }).click();
#     $(element).append(e);
# })(true);

# %%
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

# %%
prev = datetime.datetime.now()
description = spark.read.format("csv").option("inferSchema","true").option("header","true").load("Course_info.csv")
now = datetime.datetime.now()
print(now - prev)

# %%
prev = datetime.datetime.now()
comments = spark.read.format("csv").option("inferSchema","true").option("header","true").load("Comments.csv")
now = datetime.datetime.now()
print(now - prev)

# %%
df = description.limit(10).toPandas()
df.head()

# %%
prev = datetime.datetime.now()
description = description.withColumn("num_subscribers",description['num_subscribers'].cast('int'))
description = description.withColumn("num_reviews",description['num_reviews'].cast('int'))
description = description.withColumn("num_comments",description['num_comments'].cast('int'))
description = description.withColumn("num_lectures",description['num_lectures'].cast('int'))
description = spark.read.format("csv").option("inferSchema","true").option("header","true").load("Course_info.csv")
description = description.withColumn("published_time",description['published_time'].cast('timestamp'))
description = description.withColumn("last_update_date",description['last_update_date'].cast('date'))
now = datetime.datetime.now()
print(now - prev)


# %%
def get_name(url):
	if url == None:
		return ""
	return str(url[6:-1])

def get_course_name(url):
	if url == None:
		return ""
	return str(url[8:-1])

# %%
description.printSchema()

# %%
prev = datetime.datetime.now()
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
get_name_udf = udf(lambda x:get_name(x),StringType())
get_cname_udf = udf(lambda x:get_course_name(x),StringType())
# Custom UDF with withColumn()
description = description.withColumn("instructor_url", get_name_udf(col("instructor_url")))
description = description.withColumn("course_url", get_cname_udf(col("course_url")))
# rename columns
description = description.withColumnRenamed("instructor_url","instructor_username")
description = description.withColumnRenamed("course_url","course_name")
now = datetime.datetime.now()
print(now - prev)


# %%
def getFirstName(name):
	ans = ""
	if name is None:
		return "None"
	sname = name.split()
	prefix = ['mr.','mrs.','ms.','dr.','prof.','sr.','jr.','.',',','mr','mrs','ms','dr','prof','sr','jr']
	for i in range(len(sname)):
		sname[i] = sname[i].lower()
	if len(sname) > 0 and sname[0] not in prefix:
		ans = sname[0]
	elif len(sname) > 1 and sname[1] not in prefix:
		ans = sname[1]
	elif len(sname) > 2 and sname[2] not in prefix:
		ans = sname[2]
	ans =ans.capitalize()
	return ans

# %%
# user define function ========= 
prev = datetime.datetime.now()
getFirstNameUdf = udf(lambda x:getFirstName(x),StringType())
description = description.withColumn("instructor_name", getFirstNameUdf(col("instructor_name")))
description = description.withColumnRenamed("instructor_name","name")
now = datetime.datetime.now()
print(now - prev)

# %%
description.limit(5).toPandas().head()

# %%
import gender_guesser.detector as gender
gd = gender.Detector()

# %%
get_gender_udf = udf(lambda x:gd.get_gender(x),StringType())
description = description.withColumn("gender", get_gender_udf(col("name")))

# %%
description.limit(5).toPandas().head()

# %%
# a basic filtering of the information
prev = datetime.datetime.now()
known_instructors = description.filter("gender != 'unknown' ")
now = datetime.datetime.now()
print(now - prev)

# %%
known_instructors.limit(5).toPandas().head(5)

# %%
from pyspark.sql.functions import sum, max, min, avg, count, mean

# %%
# average groupby ==========
prev = datetime.datetime.now()
avg_num_subscribers_per_gender = known_instructors.groupBy('gender').agg(mean("num_subscribers"))
now = datetime.datetime.now()
print(now - prev)

# %%
# This is a HUGE inconvenient for plotting
avg_num_subscribers_per_gender.toPandas().head()

# %%
pltp = avg_num_subscribers_per_gender.toPandas()

# %%
import matplotlib.pyplot as plt
import seaborn as sns

# %%
sns.barplot(data = pltp, x ="gender", y = "avg(num_subscribers)")

# %%
avg_avg_rating = known_instructors.groupBy('gender').agg(mean("avg_rating"))

# %%
sns.barplot(data = avg_avg_rating.toPandas(), x ="gender", y = "avg(avg_rating)")



# %%

# Experimental:
# extract the name using regex

## Custom UDF with select()  
#df.select(col("Seqno"),get_name_udf(col("Name")).alias("Name") ).show(truncate=False)
#
## %%
#tmp_data = description.select(regexp_extract('instructor_url', '^/[A-Za-z]+/(\S*)/|(\s)$',1))
#
## %%
#description.printSchema()
#
## %%
#df = description.limit(20).toPandas()
#df.head()
#
## %%
#description.count()
#
## %%
#df = tmp_data.limit(20).toPandas()
#df.head()
#
## %%
#tmp_data.show(100)
