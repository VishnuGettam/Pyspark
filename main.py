#Simple project to read CSV file and display it as a dataframe table

from pyspark.sql import *;

if __name__ == '__main__':
    #creation of spark session
    spark = SparkSession\
        .builder\
        .appName('Read CSV')\
        .master('local[2]')\
        .getOrCreate();

    #read the csv file
    df_employee = spark\
        .read\
        .option('inferSchema','true')\
        .option('header','true')\
        .csv('./data/employees.csv');

    #display the csv file
    df_employee.show();

    spark.stop();


