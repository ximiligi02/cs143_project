import org.apache.spark.sql.types._

val inputPath = "/databricks-datasets/structured-streaming/events/"

// Since we know the data format already, let's define the schema to speed up processing (no need for Spark to infer schema)
val jsonSchema = new StructType().add("time", TimestampType).add("action", StringType)

val staticInputDF = 
  spark
    .read
    .schema(jsonSchema)
    .json(inputPath)

import org.apache.spark.sql.functions._

// Similar to definition of staticInputDF above, just using `readStream` instead of `read`
val streamingInputDF = 
  spark
    .readStream                       // `readStream` instead of `read` for creating streaming DataFrame
    .schema(jsonSchema)               // Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  // Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)

// Same query as staticInputDF
val streamingCountsDF = 
  streamingInputDF
    .groupBy($"action", window($"time", "1 hour"))
    .count()

// Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

def userGen(time: java.sql.Timestamp): String = {
  val t = time.getTime % 3
  if (t == 1) "Alice"
  else if (t == 2) "Bob"
  else "Charlie"
}

def genRequest(time: java.sql.Timestamp): String = {
  val t = time.getTime % 60
  if (t < 20) "GET"
  else if (t < 50) "POST"
  else  "PUT"
}

def genStatus(time: java.sql.Timestamp): Int = {
  val t = time.getTime % 1000
  if (t < 400) 200
  else if (t < 700) 300
  else if (t < 900) 400
  else 500
}

def genContentSize(time: java.sql.Timestamp): Long = {
  time.getTime % 4096
}

// Same query as staticInputDF
val streamingLogInputDF = 
  streamingInputDF
    .withColumn("user", udf(userGen _).apply($"time") as 'user)
    .withColumn("request", udf(genRequest _).apply($"time") as 'request)
    .withColumn("status", udf(genStatus _).apply($"time") as 'status)
    .withColumn("size", udf(genContentSize _).apply($"time") as 'size)

// Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small
val streamingAvgContentSizeDF =
streamingLogInputDF
.groupBy($"user", window($"time", "1 hour"))
.agg(avg("size").alias("avg_size"))

val query =
  streamingAvgContentSizeDF
    .writeStream
    .format("memory")        // memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("avgcs")      // counts = name of the in-memory table
    .outputMode("complete")  // complete = all the counts should be in the table
    .start()

%sql select user, date_format(window.end, "MMM-dd HH:mm") as time, avg_size from avgcs order by time, user

spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small

val streamingRequestCountDF =
  streamingLogInputDF
    .groupBy($"request", window($"time", "1 hour"))
    .count()

val query =
  streamingRequestCountDF
    .writeStream
    .format("memory")        // memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("requests")     // counts = name of the in-memory table
    .outputMode("complete")  // complete = all the counts should be in the table
    .start()

%sql select request, date_format(window.end, "MMM-dd HH:mm") as time, count from requests order by time, request

spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small

val streamingUserPostErrorCountDF =
  streamingLogInputDF
    .filter($"request" === "POST"&&$"status" === "200")
    .groupBy($"user",  window($"time", "1 hour"))
    .count()

val query =
  streamingUserPostErrorCountDF
    .writeStream
    .format("memory")        // memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("post_error")     // counts = name of the in-memory table
    .outputMode("complete")  // complete = all the counts should be in the table
    .start()

%sql select user, date_format(window.end, "MMM-dd HH:mm") as time, count from post_error order by time, user