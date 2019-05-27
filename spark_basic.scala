
//// Basic Spark Operations

//// a. Create a Spark data frame from a CSV file which has the headers in the first row


// read: return a DataFrameReader that can be used to read non-streaming data in as a DataFrame
// option(key: String, value: Boolean) : add input option for the underlying data source

val df = spark.read.option("header", "true").csv("Datasets/simple.csv")

// verify dataframe
df.show()


//// b. Print the data frame’s schema

df.printSchema()

//// c. Convert the data frame to a RDD and display its contents

// convert DataFrame to RDD 
// use org.apache.spark.sql.Row in the argument to prevent type not found 

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
val myRdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = df.rdd


// display content
// use collect opeartion on the RDD to return all the elements of the dataset as an array
// iterate through all the element. print the string
myRdd.collect().foreach(println)

//// d. Create a RDD by reading from a text file

// use SparkContext's textFile method to create a text file RDD from file 
val smallText = sc.textFile("/home/user/spark-2.2.1-bin-hadoop2.7/README.md")


//// e. Calculate the total length in characters, including white spaces, for all the lines in the $SPARK_HOME/README.md file.

// map : return the length of each element in the RDD with x.length
// reduce : sum up all the length in the result and return the total. first result is passed on to the next operation and added together with the new value
println("Total length in characters (including whitespace): " + smallText.map(x=>x.length).reduce(_+_))


//// f. Count and display all the words as (String, Int) pairs, which occur in $SPARK_HOME/README.md file

// flatMap : split lines on whitespace to get collections of words and flattens them into a single RDD of results
// map : map the words in the RDD to (key,value) pair to represent each word has 1 count to start with
// reduceByKey : apply summation on two parameters of the same key and return a sum 
// _+_ : the interim result will be added together with the next element in the collection 
val counts = smallText.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)

// collect : return all the elements of the dataset as an array
// foreach(println) : iterate through all the element. print the string.
counts.collect().foreach(println)


//// g. Write a program which does word count of the $SPARK_HOME/README.md file using Spark


// map : map the second element in the tuple (which is the count) into a RDD of results
// reduce : sum up the two parameters in the RDD and return the total 
// _+_ : the interim total will be added together with the next element in the collection, finally return a single value

val wc = counts.map(x=>x._2).reduce(_+_)
println("Total word count in README.md file: " + wc)


//// h. Write a compact program, which computes the factorials of an integer array X(1,2,3,4,5) and then sums these up into a single value

// create a recursive factorial function that calculat the factorial of an integer 
def factorial(n:Int):Int = if (n==0) 1 else n * factorial(n-1)

val input = Array(1,2,3,4,5)
// parallelize : create a new distributed data set with partitions and the elements of the collection are copied to the distributed dataset 
// map : apply factorial function on each element in the input array
// reduce : sum up the 5 interim results by taking two parameters in the RDD and return the total 
val sum = sc.parallelize(input).map(x=>factorial(x)).reduce(_+_)
println("Factorials of (1,2,3,4,5) is " + sum)







