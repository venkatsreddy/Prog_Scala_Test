# Prog_Scala_Test

Instructions on how to execute

1. Need to create a sample CSV file with the provided data and test batch aggregation job with it.

Save file with name sample_data.csv:

metric,value,timestamp
temperature,88,2022-06-04T12:01:00.000Z
temperature,89,2022-06-04T12:01:30.000Z
precipitation,0.5,2022-06-04T14:23:32.000Z

2. Now, you can run the batch aggregation job with this sample data. Assuming you've compiled your Scala code into a JAR file named BatchAggregationJob.jar, you can run it using the following command:

spark-submit --class BatchAggregationJob --master local[*] BatchAggregationJob.jar sample_data.csv output_data

3. Make sure to replace BatchAggregationJob.jar with the actual name of your JAR file, sample_data.csv with the path to your sample data file, and output_data with the desired output directory path.

After running the job, you should find the aggregated results in the specified output directory. You can inspect the output files to verify that the aggregation has been performed correctly or not.




