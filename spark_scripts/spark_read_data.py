from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf, array_sort
import boto3
import os
import pandas as pd

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
spark = SparkSession(sc).builder.appName('s3toSpark').getOrCreate()

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
my_bucket = s3.Bucket('pinterest-data-1d58b717-eb31-4af7-bfd6-3f103a337965')

# List 10 objects from bucket and store their full directories in a list
bucket_dir = "s3a://pinterest-data-1d58b717-eb31-4af7-bfd6-3f103a337965"
retrieved_objects_dir_list = []
for file in my_bucket.objects.all():
    if len(retrieved_objects_dir_list) < 10:
        retrieved_objects_dir_list.append(f"{bucket_dir}/{file.key}")

# access the list of obeject directories
df = spark.read.option('multiline','true').json(retrieved_objects_dir_list)
df.show(1,True)

# clean data by: 
# 1. converting tag_list from str to array
# 2. converting follower_count from str to int

# Function to replace '4k' with 4000 or 4M with 4000000
def replace_k_or_M(number):
    if 'k' in number or 'M' in number:
        number = number.replace('k','000').replace('M','000000')
    new_number = int(number)
    return(new_number)

# Convert function so that it can be applied on pyspark column
replace_k_or_M_udf = udf(lambda z: replace_k_or_M(z))

# Implement both functions with cast schema for follower_count
df2 = df.withColumn('tag_list',array_sort(split(col('tag_list'),',')))\
.withColumn('follower_count',replace_k_or_M_udf(col('follower_count')).cast('int'))

df2.show(vertical=True)






