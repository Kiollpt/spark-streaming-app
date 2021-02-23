import java.sql.Timestamp

import com.google.gson.Gson
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{
  DataFrame,
  Encoders,
  ForeachWriter,
  Row,
  SparkSession
}

case class Taxi(zone: String,
                choroplethData: String,
                dateTime: Timestamp,
                event: String)

case class LeftletData(zone: String,
                       choroplethData: String,
                       sup_dem_ratio: Double)

object taxiapp {

  def main(args: Array[String]): Unit = {

    val filePath = "src/resource/sample-data"
    val ss = SparkSession
      .builder()
      .master("local[1]")
      .appName("taxi-app")
      .getOrCreate()
    //ss.conf.set("spark.sql.streaming.schemaInference","true")
    ss.sparkContext.setLogLevel("WARN")

    import ss.implicits._

    val taxiSchema = Encoders.product[Taxi].schema
//
//    val taxiDF =
//      ss.readStream.option("header", "true").schema(taxiSchema).csv(filePath)

    val kafkaDF = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "taxi")
      .load()

    val taxiDF = kafkaDF
      .select(from_json($"value".cast("string"), taxiSchema) as "record")
      .select("record.*")

    println(taxiDF.isStreaming)

    val enrichedStreamingDf = taxiDF.transform(withSupplyDemand())

    val ratioDF = ratioDataFrame(enrichedStreamingDf)

    ratioDF.writeStream
      .outputMode("append")
      .foreach(new ForeachWriter[Row] {

        def open(partitionId: Long, version: Long): Boolean = {
          true
        }

        def process(value: Row): Unit = {

          val emptyRatio = Option(value(3)).getOrElse(0)

          if (emptyRatio != 0) {

            val r =
              LeftletData(
                value.getString(1),
                value.getString(2),
                value.getDouble(3)
              )
            val js = new Gson().toJson(r)
            requests.post(
              "http://localhost:2020/api/v1.0/traffic",
              data = js,
              headers = Map("Content-Type" -> "application/json")
            )

          }

        }

        def close(errorOrNull: Throwable): Unit = {
          // Close the connection

        }
      })
      .start()
      .awaitTermination()

  }

  def withSupplyDemand()(df: DataFrame): DataFrame = {

    df.withColumn(
        "demand",
        when(col("event") === "PICKUP", lit(1))
          .when(col("event") === "DROPOFF", lit(0))
          .otherwise(lit(0))
      )
      .withColumn(
        "supply",
        when(col("event") === "PICKUP", lit(1))
          .when(col("event") === "DROPOFF", lit(1))
          .otherwise(lit(0))
      )

  }
  def ratioDataFrame(df: DataFrame): DataFrame = {

    df.withWatermark("dateTime", "10 minutes")
      .groupBy(
        window(col("dateTime"), "10 minutes", "5 minutes"),
        col("zone"),
        col("choroplethData")
      )
      .agg(
        (sum(col("supply")) / sum(col("demand")))
          .as("sup_dem_ratio")
      )

  }

}
