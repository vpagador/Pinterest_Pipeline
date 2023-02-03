from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, MapType, ArrayType
from pyspark.sql.functions import col ,json_tuple, udf, array_sort, split
import json
import os

class Spark_stream:

    def __init__(self):
        # Submit spark sql kafka package from Maven repo to pyspark
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 streaming_consumer.py pyspark-shell'
        # Specify kafka topic to stream data from
        self.kafka_topic = 'pinterestPosts'
        # Specify your kafka server to read from
        self.kafka_bootstrap_servers = 'localhost:9092'

        self.spark = SparkSession.builder.appName("kafkaStreaming").getOrCreate()

        # Display error messages
        self.spark.sparkContext.setLogLevel('ERROR')
            
        # Set schema for columns
        self.schema = StructType([
            StructField('category',StringType(),True),
            StructField('index',IntegerType(),True),
            StructField('unique_id',StringType(),True),
            StructField('title',StringType(),True),
            StructField('description',StringType(),True),
            StructField('follower_count',StringType(),True),
            StructField('tag_list',StringType(),True),
            StructField('is_image_or_video',StringType(),True),
            StructField('image_src',StringType(),True),
            StructField('downloaded',BooleanType(),True),
            StructField('save_location',StringType(),True)
        ])
        

    def start_read_stream(self):
        # Read raw values from stream into pyspark dataframe
        stream_df = self.spark \
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.kafka_bootstrap_servers) \
            .option('subscribe', self.kafka_topic) \
            .option('startingOffsets', 'earliest') \
            .load()
        return(stream_df)

    def read_stream_data_values(self, df):
        # Select the value part of the kafka message and cast it to a string
        df = df[['value']].withColumn('value',col('value').cast('string'))
        return(df)
    
    # clean data by: 
    # 1. converting tag_list from str to array
    # 2. converting follower_count from str to int
    def udf_conversion(replace_k_or_M):
        def replace_k_or_M(number):
            if 'k' in number or 'M' in number:
                number = number.replace('k','000').replace('M','000000')
            new_number = int(number)
            return(new_number)
        replace_k_or_M_udf = udf(lambda new_number: replace_k_or_M(new_number))
        return(replace_k_or_M_udf)
    
    def clean_stream_data(self, df):
        # Apply data cleaning:
        df2 = df.select(json_tuple(col('value'),'category','index','unique_id','title','description','follower_count',
        'tag_list','is_image_or_video','image_src','downloaded','save_location')) \
        .toDF('category','index','unique_id','title','description','follower_count',
        'tag_list','is_image_or_video','image_src','downloaded','save_location')

        # Apply cleaning functions and change schema
        udf_func = self.udf_conversion()
        # Implement both functions with cast schema for follower_count
        df2 = df2.withColumn('tag_list',array_sort(split(col('tag_list'),','))) \
        .withColumn('follower_count',udf_func(col('follower_count')))
        return(df2)
    
    def process_batch_aggregations():
        # Define aggregation functions to category, follower count and sort by either.
        pass
    
    def write_stream_to_console(self):
        # Write messages to local directory
        stream_df = self.start_read_stream()
        stream_df = self.read_stream_data_values(stream_df)
        stream_df = self.clean_stream_data(stream_df)  
        stream_df = stream_df.writeStream \
            .format('console') \
            .outputMode('append') \
            .option('truncate', 'true') \
            .start() \
            .awaitTermination()
    
    def write_stream_with_aggregations(self):
        # Apply aggregation function to write stream
        pass
    
    def stream_clean_data(self):
        stream_df = self.start_read_stream()
        

if __name__ == '__main__':
    stream = Spark_stream()
    stream.write_stream_to_console()