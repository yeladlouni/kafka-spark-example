import org.apache.spark.sql.SparkSession

object Example extends App {
  val spark = SparkSession
    .builder()
    .appName("KafkaExample")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "covid")
    .load()

  val query = df.select("offset", "partition", "timestamp", "value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "c:/tmp/spark")
    .option("topic", "newtopic").start()

  query.awaitTermination()

}