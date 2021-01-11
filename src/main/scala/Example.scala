import org.apache.spark.sql.SparkSession

object Example extends App {
  val spark = SparkSession
    .builder()
    .appName("KafkaExample")
    .config("spark.master", "local")
    .getOrCreate()

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "covid")
    .load()

  println(df.head())


}