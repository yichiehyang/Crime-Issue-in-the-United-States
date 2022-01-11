# Crime Issue in the United States

My project is focused on criminal incidence in the United States.   
In this project, I would try to find out which state may have the highest crime rate and to know the factors of the high crime rate. After reading some pieces of information about criminality in the United States, I suggested that the unemployment rate and educational level may be the two main factors that influence the criminal rate.

### Data Sources

1. United States crime rates by county
This data can be downloaded as a csv file. There are 24 columns and 3236 rows which cover the crime rate per 100000 people, county name with state code, population of the county, and the count of different kinds of crime acts. I
decided to use the state code and recalculate the crime rate for each state using the population and the crime rate in each county. At last, I will get the state code with the crime rate for each state.    
The data is found on Kaggle: https://www.kaggle.com/mikejohnsonjr/united-states-crime-rates-by-county

2. USA Unemployment
This data can be downloaded as a csv file. There are 93 columns and 3275 rows which cover county, state, labor force count, unemployment count, employment count, and unemployment rate from 2000 to 2020. It also covers the data that calculate the unemployment rate for each state, so I used this
data with the year of 2000 for the analysis afterwards.
The dataset is retrieved from Kaggle:
https://www.kaggle.com/valbauman/student-engagement-online-learning-supplement?select=unemployment.csv

3. USA Education Level
 This data can be downloaded as a csv file. There are 48 columns and 3283 rows which covers county, state, count of adults less than high school
diploma, count of adults only have high school diploma, count of adults with 3 years’ college experience, count of adults with four years college or higher, percentage of adults less than high school diploma, percentage of adults only have high school diploma, percentage of adults with 3 years’ college experience, percentage of adults with four years college or higher for the following years: 1970, 1980, 1990, 2000, 2019. I used county, state, count of adults less than high school diploma for each county, percentage of adults less than high school diploma for each county with the year 2000 to get the data of low-level education rate. Then, I recalculated the education rate for
each state.
The data is retrieved from Kaggle:
https://www.kaggle.com/valbauman/student-engagement-online-learning- supplement?select=education.csv

### Challenges
I have encountered many challenges while preprocessing the data. I have mentioned all the challenges before. The first challenge was that I realized I need to recalculate the education rate and crime rate again which I had never think of while writing the proposal. I could not aggregate all the rate and divided by the number of the county for each state because the rate is related to the population of each county. And the population of each county is not the same. Therefore, I did the recalculation to solve this challenge.
The next trouble was that the output from MRJob became a same column when I uploaded into the PySpark table. I have tried many ways to separate the columns and finally succeeded. Then, I found out I need to remove the quotation mark and the spaces in the column in order to make the state columns look the same. I searched for many ways and looked for answer in the documentation of PySpark. Eventually, I got the result that I desired.