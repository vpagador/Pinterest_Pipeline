from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_sort, col, split, udf
import boto3
import os
import pandas as pd

class Spark_Clean:

    def __init__(self):
        # Add packages required to access aws s3
        os.environ["PYSPARK_SUBMIT_ARGS"]= "--packages com.amazonaws:aws-java-sdk-s3:1.12.389,\
org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"


        # Create spark configuration
        conf = (
            SparkConf() \
            # Setting application name
            .setAppName("s3toSpark") \
            .setMaster('local[*]')
        )

        sc = SparkContext(conf=conf)

        # Start spark session
        self.spark = SparkSession(sc).builder.appName('s3toSpark').getOrCreate()

        # file path to aws_credentials
        aws_cred = pd.read_csv('/home/van28/Desktop/AiCore/aws_cred.txt',sep=" ",header=None,names=None)
        accessKeyId=aws_cred.values[0][0]
        secretAccessKey=aws_cred.values[1][0]

        # Configure setting to access s3 bucket
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key',accessKeyId)
        hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') 

        # Access s3 bucket to list contents
        s3 = boto3.resource('s3')
        # view files in bucket
        self.my_bucket = s3.Bucket('pinterest-data-1d58b717-eb31-4af7-bfd6-3f103a337965')
        self.retrieved_objects_dir_list = []

    def retrieve_object_dir(self):
        # List 10 objects from bucket and store their full directories in a list
        bucket_dir = "s3a://pinterest-data-1d58b717-eb31-4af7-bfd6-3f103a337965"
        for file in self.my_bucket.objects.all():
            if len(self.retrieved_objects_dir_list) < 10:
                self.retrieved_objects_dir_list.append(f"{bucket_dir}/{file.key}")
    
    def spark_read(self):
        # access the list of obeject directories
        df = self.spark.read.option('multiline','true').json(self.retrieved_objects_dir_list)
        df.show(1,True)
        return(df)

    # clean data by: 
    # 1. converting tag_list from str to array
    # 2. converting follower_count from str to int

    def udf_conversion(replace_k_or_M):
        def replace_k_or_M(number):
            if 'k' in number or 'M' in number:
                new_number = number.replace('k','000').replace('M','000000')
                return(new_number)
        replace_k_or_M_udf = udf(lambda new_number: replace_k_or_M(new_number))
        return(replace_k_or_M_udf)
    
    def spark_read_clean(self):
        udf_func = self.udf_conversion()
        df = self.spark_read()
        # Implement both functions with cast schema for follower_count
        df2 = df.withColumn('tag_list',array_sort(split(col('tag_list'),',')))\
        .withColumn('follower_count',udf_func(col('follower_count')).cast('int'))
        df2.show(vertical=True)

    def spark_job(self):
        self.retrieve_object_dir()
        self.spark_read_clean()


if __name__ == "__main__":
    job = Spark_Clean()
    job.spark_job()






