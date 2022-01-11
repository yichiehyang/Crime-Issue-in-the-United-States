from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext(appName="project1")
sqlContext = SQLContext(sc)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *

# preprocess unemployment table
unemployment =  sqlContext.read.csv("unemployment.csv", header=True)
unemployment.registerTempTable("unemployment")

unemploy = sqlContext.sql("SELECT FIPS_Code, State, Unemployment_rate_2000 as unemploy_rate from unemployment where FIPS_Code like '%000' and FIPS_Code!='00000'")
unemploy.registerTempTable("unemploy")


#preprocess crime table
crime=  sqlContext.read.csv("crime_output.csv", header=False)
crime.registerTempTable("crime")
spilt_col = f.split(crime['_c0'],'\t')
crime=crime.withColumn('State', spilt_col.getItem(0)).withColumn('crimeRate',spilt_col.getItem(1))
crime = crime.withColumn('State', regexp_replace('State', '"', ''))
crime.registerTempTable("crime")

#preprocess education table
education = sqlContext.read.csv("edu_output.csv", header=False)
education.registerTempTable('education')
#education.printSchema()
spiltCol = f.split(education['_c0'],'\t')
education=education.withColumn('State', spiltCol.getItem(0)).withColumn('edu_rate',spiltCol.getItem(1))
education = education.withColumn('State', regexp_replace('State', '"', ''))
education.registerTempTable('education')


#question 1 : which state may have the highest crime rate
c = sqlContext.sql("SELECT State, crimeRate from crime order by crimeRate desc")
c.collect()
c.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('crime_rate_output')

#education and crime rate join table
edu_crime = sqlContext.sql("SELECT c.State, c.crimeRate, e.edu_rate FROM crime as c join education as e on trim(e.State)=trim(c.State)")
edu_crime.collect()
#edu_crime.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('education_crime_output')

#unemployment and crime rate join rable
unemploy_crime = sqlContext.sql("SELECT distinct c.State, c.crimeRate, u.unemploy_rate FROM crime as c join unemploy as u on trim(u.State)=trim(c.State)")
unemploy_crime.collect()
#unemploy_crime.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('unemploy_crime_output')
