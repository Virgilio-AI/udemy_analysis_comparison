# udemy_analysis_comparison
This is a data analysis repository to compare Pandas, Spark and Koalas

# Introduction
given there are a lot of tools for big data, choosing the correct one can be a picky question.
- what are the advantages of each one?
- what is faster?
- if one is faster than the other, is it significally faster or exponential?
- which one is more convenient, ( easier syntax, which kind of syntax do people know already )

I did the same project on all of the flavours. a simple data manipulation of the udemy dataframe to make a comparisson
of the best and the most succesfull teachers based on gender ( which had to be extracted from the name of the instructor )


# loading csv's
first compare loading a big csv file ( you can check each individual notebook )
|  toCompare |  pandas | koalas  | pyspark  |
|---|---|---|---|
|  time |  18s | 2s  |  1s |
| convenient  | yes  | 1/2  | 1/2  |

koalas was not very convenient because it doen't has all the parameters that pandas usually has.
this is a paramter that is available on pandas but not on Koalas
![image](https://user-images.githubusercontent.com/59902976/210259862-8f881c0b-bea7-423e-ba72-6fca0659cd5d.png)

In pyspark it is not very convenient because of all the REQUIRED parameters used to read a simple csv file.
also is a syntax that nobody uses

![image](https://user-images.githubusercontent.com/59902976/210260244-a41b5d73-9b00-4085-adf6-57237909bbaf.png)
this shows the biggest disadvantage of pyspark. which is inconvenience with the syntax.


# simple group by
group by function comparisson

|  toCompare | pandas | Koalas         | pyspark |
|------------|--------|----------------|---------|
| time       |18s     |0.03s           |0.009s   |
| convenient |yes     |yes             |yes      |

actually it is very convenient in every flavour, given that it is very similar. check the individual notebooks!

# user defined function
for a user defined function on a string. here is a table
# user defined functions applied to a column in the dataframe

| toCompare | pandas | koalas | pyspark |
|-----------|--------|--------|---------|
|time       |18s     |5s      |1s       |
|convenient |yes     |1/2     |no       |

Pandas is very convenient. bacause not only does it provides the .map function, it also provides str functions like this 
![image](https://user-images.githubusercontent.com/59902976/210263232-2e3ced7f-64b3-4ec0-9474-3fa947fd204d.png)
unlike koalas. koalas doesn't allows the str methods. but it does has the .map function.
pyspark actually is actually not so inconvinient. but it doesnt uses the pandas syntax at all and also requires you
to load the udf function 

![image](https://user-images.githubusercontent.com/59902976/210263731-7dcb8cf7-f845-4fbf-b700-a31fd4da1aff.png)

# visualization
| toCompare | pandas | koalas | pyspark |
|-----------|--------|--------|---------|
|convenient |yes     |no      |no       |

now the visualization is also important. in pyspark you HAVE to convert to pandas to 
plot your data, koalas is also not convenient at all and pandas has ALL the plotting libraries

# conclusion
In conclusion koalas is not actually ready to use as for now, compared to pyspark I would use pyspark any time of the year.

yes it has a different syntaxt. but when you get used to it you can manipulate information MUCH faster than pandas, and a little faster than
koalas. also koalas is not up to date with the newest python version. hence it was not very convenient for me because I use a rolling release
Linux distribution called Water Linux. check it out at [https://www.archwater.org](archwater.org). so I had to install a different version of python
and re download all packages just so that koalas could work. also even then I had to downgrade manually other packages.
I think the best option thus far is pyspark. when you need to plot your information you can convert to pandas. 
also pyspark has machine learning libraries that are VERY convenient to use because
they are MUCH faster than doing machine learnig using xgboost or sklearn.

