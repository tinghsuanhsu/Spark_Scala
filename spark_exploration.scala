
//// Data Exploration with Spark 

// use Spark SQL API to achieve the task
// import library to use DataFrame.sort(desc("col1"))
import org.apache.spark.sql.functions._

// Create the entry point into all functionality in Spark SQL 
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// create a dataframe from file flight_2008_pq.parquet
val flight = sqlContext.read.parquet("/home/user/Documents/Datasets/flight_2008_pq.parquet")

// check schema : all are string 
flight.printSchema()

// cast integer type to column DepDelay for aggregation purpose 
val newflight = flight.withColumn("DepDelay", flight("DepDelay").cast("integer"))

// check schema again : DepDelay is in integer, can be aggregated
newflight.printSchema()

// get the maximum DepDelay for each FlightNum
// rename the column to DepDelay for display purpose 
// only keep columns that provides relevant information for decision making
val delay = newflight.groupBy("UniqueCarrier","FlightNum","Origin","Dest").max("DepDelay").withColumnRenamed("max(DepDelay)","DepDelay")

// display 20 flights
delay.show(20)

// display the top 20 maximum DepDelay in descending order
delay.sort(desc("DepDelay")).show(20)
