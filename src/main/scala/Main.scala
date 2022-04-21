import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration.DurationInt

object Main extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", ":9092")
    .option("subscribe", "another_topic")
    .load()
  val newdf = df.withColumn("value", upper($"value"))
  newdf.writeStream.format("console").trigger(Trigger.ProcessingTime(5.seconds)).start.awaitTermination()

}
