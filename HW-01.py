#######################
Venakt Koushik Muthyapu
CS 467
Hw-01
######################


# COMMAND-1 Description
spark.conf.set("spark.sql.shuffle.partitions", 5)
static = spark.read.json("data/activity-data")

# Here in this command we are doing two things. Firstly we are doing 5 shuffle partitions where our Rdds are partioned into 5 parts and shuffled,
# we do this in order to perform parallel processing. and then the second thing is we are load out data folder into a variable called static.


# COMMAND-2 Description
streaming = spark.readStream.schema(static.schema).option("maxFilesPerTrigger", 1)\
  .json("data/activity-data")

# Every data file has a schema, our data folder that we have loaded above also has a data schema.
# In the second command we are reading the data as a stream and streaming it from the static dataframs into streaming dataframs, only if our data schema matches with out loaded folder's data schema.
# The maxFilesPerTrigger is way you are deciding how many files you want read at once, here we are reading 1 file at once.


# COMMAND-3 Description
activityCounts = streaming.groupBy("gt").count()

# In this command we are performing a simple action. We are using the action groupBy on the "gt" column or variable and counting using count().
# The groupBy function groups are the similar values in the "gt" column and counting how many times each value is repeated and putting the result in activityCounts variabel.
# gt colounm contains the activity being performed by the user at that point in time.


# COMMAND-4 Description
activityQuery = activityCounts.writeStream.queryName("activity_counts")\
  .format("memory").outputMode("complete")\
  .start()

# For this command we are performing an action where we are executing the activityCounts action on the incoming streaming data and naming the query as activity_counts and store it in in-memory table.
# For the destination of the query we are using memory sink which is for debugging and we are using complete mode as the mode of our output which rewrites the output every single time.

# COMMAND-5 Description
from time import sleep
for x in range(5):
    spark.sql("SELECT * FROM activity_counts").show()
    sleep(1)

# In this command we are writing a query to output our in-memory table activity_counts after every second until five seconds.
# We will see 5 different tables as we are using complete output mode.

# COMMAND-6 Description
from pyspark.sql.functions import expr
simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))\
  .where("stairs")\
  .where("gt is not null")\
  .select("gt", "model", "arrival_time", "creation_time")\
  .writeStream\
  .queryName("simple_transform")\
  .format("memory")\
  .outputMode("append")\
  .start()

# Here we are performing a simple transformation where we are taking only the values which contains stairs word in them from the gt colounm and also gets the values related to it in the columns model, arriaval_time and creation_time and naming this table as simple_transformation. here we are using memory as sink destination and append as output-mode.
# Append output mode appends data to the existing table rather than rewriting it.
# We will see a table with columns gt, model, arrival_time, creation_time and all the values of the columns correspond to only activities that contain stairs word in it.


# COMMAND-7 Description
deviceModelStats = streaming.cube("gt", "model").avg()\
  .drop("avg(Arrival_time)")\
  .drop("avg(Creation_Time)")\
  .drop("avg(Index)")\
  .writeStream.queryName("device_counts").format("memory")\
  .outputMode("complete")\
  .start()

# In this command we are performing an aggregation to get a summary table for activities and the device model. We are dropping the averages of Arrival_time, Creation_Time and Index, and only keeping the averages x,y,z sensors and making a cube of activities, phone model and the averages of x,y,z.
# We will see a summary table with columns gt, model, avg(x), avg(y), avg(z).

# COMMAND-8 Description
historicalAgg = static.groupBy("gt", "model").avg()
deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")\
  .cube("gt", "model").avg()\
  .join(historicalAgg, ["gt", "model"])\
  .writeStream.queryName("device_counts").format("memory")\
  .outputMode("complete")\
  .start()

# In this command we are firstly averaging all the columns and arranging them based on activities and device models and putting into a variable historicalAgg.
# Now we have started a new variable named deviceModelStats in which we are first dropping the "Arrival_Time", "Creation_Time", "Index" columns and then averaging the remaining columns and arranging them based on activities and device models. Then we are joining all the columns in historicalAgg to it and naming the query as device_counts and we are giving output destination as memory and output-mode as complete.
# We will see a table in which we will see a avg(x), avg(y), avg(z) twice since we are joining a table which has those columns.

# COMMAND-9 Description
streaming = spark\
.readStream\
.schema(static.schema)\
.option("maxFilesPerTrigger", 10)\
.json("data/activity-data")

# Every data file has a schema, our data folder that we have loaded above also has a data schema.
# In the this command we are reading the data as a stream and streaming it from the static dataframs into streaming dataframs, only if our data schema matches with out loaded folder.
# The maxFilesPerTrigger is way you are deciding how many files you want read at once, here we are reading 8 file at once and processing them parallel.

# COMMAND-10 Description
withEventTime = streaming.selectExpr(
  "*",
  "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

# Here in this command we are taking the values of Creation_Time column as it streams and manipulating its values to convert the timestamp column into the proper Spark SQL timestamp type. and changing the Creation_Time column name to event_time

# COMMAND-11 Description
from pyspark.sql.functions import window, col
withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()\
  .writeStream\
  .queryName("pyevents_per_window")\
  .format("memory")\
  .outputMode("complete")\
  .start()

# in this command we will count the number of streaming values from event_time columns for every 10 mins and naming the query as pyevents_per_window. The output destination is memory and mode is complete.
# In this command we are going to get a table with window and count. window shows time and count is how many values are streamed from that columns in given time window.

# COMMAND-12 Description
from pyspark.sql.functions import window, col
withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()\
    .writeStream\
    .queryName("pyevents_per_window")\
    .format("console")\
    .outputMode("complete")\
    .start()
# in this command we will count the number of streaming values from event_time columns for every 10 mins and naming the query as pyevents_per_window. The output destination is console which is used for testing and mode is complete.
# In this command we are going to get a table with window and count. window shows time and count is how many values are streamed from that columns in given time window.


# COMMAND-13 Description
from pyspark.sql.functions import window, col
withEventTime.groupBy(window(col("event_time"), "10 minutes"), "User").count()\
  .writeStream\
  .queryName("pyevents_per_window")\
  .format("memory")\
  .outputMode("complete")\
  .start()

# In this command we will count the number of streaming values from event_time columns for each user for every 10 mins and naming the query as pyevents_per_window. The output destination is memory and mode is complete.
# In this command we are going to get a table with window, user and count. window shows time, count is how many values are streamed from that column for the specific user in given time window.

# COMMAND-14 Description
from pyspark.sql.functions import window, col
withEventTime.groupBy(window(col("event_time"), "10 minutes", "5 minutes"))\
  .count()\
  .writeStream\
  .queryName("pyevents_per_window")\
  .format("memory")\
  .outputMode("complete")\
  .start()

# Here in this command we are counting the values of the event_time column for every 10mins but here we are doing it with an overlap as we are stating the count for every 5mins and naming the query as pyevents_per_window. The output destination is memory and mode is complete.
# In this command we are going to get a table with window and count. window shows time and count is how many values are streamed from that columns in given time window.


# COMMAND-15 Description
from pyspark.sql.functions import window, col
withEventTime\
  .withWatermark("event_time", "30 minutes")\
  .groupBy(window(col("event_time"), "10 minutes", "5 minutes"))\
  .count()\
  .writeStream\
  .queryName("pyevents_per_window")\
  .format("memory")\
  .outputMode("complete")\
  .start()

# Here in this command we are performing a Watermark streaming we are firstly initializing a water-mark of 30 mins and then counting the values of event_time column for 10mins, after 10 mins we are still waiting until 30mins looking if any more values are to come and naming the in-memory query as pyevents_per_window. he output destination is memory and mode is complete.
#In this command we are going to get a table with window and count. window shows time and count is how many values are streamed from that columns in given time window.

# COMMAND-15 Description
from pyspark.sql.functions import expr
withEventTime\
  .withWatermark("event_time", "5 seconds")\
  .dropDuplicates(["User", "event_time"])\
  .groupBy("User")\
  .count()\
  .writeStream\
  .queryName("pydeduplicated")\
  .format("memory")\
  .outputMode("complete")\
  .start()

# In this command we are getting number of events per user without any duplicates we have put the water-mark as 5 seconds so we will wait for 5 extra seconds to get any data that is streaming late.
# in this table we will see only two columns User and count.
