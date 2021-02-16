import java.sql.Timestamp

import com.google.gson.Gson
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.{DataFrame,Dataset}

case class order(`zone`:String,choroplethData:String,datetime:Timestamp,event:String)
case class result(`zone`:String,choroplethData:String,sup_dem_ratio:Double)



object taxiapp {

  def main(args: Array[String]): Unit = {

      val filePath = "src/resource"
      val ss = SparkSession.builder().master("local[1]").appName("taxi-app").getOrCreate()
      //ss.conf.set("spark.sql.streaming.schemaInference","true")
      ss.sparkContext.setLogLevel("WARN")

      import ss.implicits._

      val schema = Encoders.product[order].schema
      val df = ss.readStream.option("header","true").schema(schema).csv(filePath)

      println(df.isStreaming)


    val enrichedStreamingDf = richDataFrame(df)



    val ratioDf = enrichedStreamingDf
      .withWatermark("datetime", "10 minutes")
      .groupBy(
        window($"datetime", "10 minutes", "5 minutes"),
        $"zone",
        $"choroplethData"

      )
      .agg((sum($"supply") / sum($"demand")).as("sup_dem_ratio"))

    val d = ratioDf.writeStream.outputMode("update").format("console").start()

      d.awaitTermination()


    //
//      ratioDf.writeStream.outputMode("update").foreach(
//              new ForeachWriter[Row] {
//
//
//              def open(partitionId: Long, version: Long): Boolean = {
//                  true
//              }
//
//              def process(value: Row): Unit = {
//
//                val empty_ratio = Option(value(3)).getOrElse(0)
//
//                if (empty_ratio!=0){
//
//                  val r = result(value.getString(1),value.getString(2),value.getDouble(3))
//                  val js = new Gson().toJson(r)
//                  requests.post("http://localhost:2020/api/v1.0/traffic",
//                    data = js,
//                    headers = Map("Content-Type" -> "application/json"))
//
//                }
//
//              }
//
//              def close(errorOrNull: Throwable): Unit = {
//                // Close the connection
//
//              }
//            }).start().awaitTermination()


  }

  def richDataFrame(df:DataFrame): DataFrame= {


    val df1 = df.withColumn("demand",
      when(col("event") === "PICKUP", lit(1))
        .when(col("event") === "DROPOFF", lit(0))
        .otherwise(lit(0)))
      .withColumn("supply",
        when(col("event") === "PICKUP", lit(1))
          .when(col("event") === "DROPOFF", lit(1))
          .otherwise(lit(0)))
    df1

  }




}
