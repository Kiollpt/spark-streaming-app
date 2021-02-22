import org.apache.spark.sql.{SparkSession, SQLContext}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.Row

import taxiapp.{richDataFrame, ratioDataFrame}
case class Event(event: String)

class firstTest extends AnyFunSuite with DataSetCompare {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("test")
    .getOrCreate()

  import spark.implicits._
  implicit val sqlCtx: SQLContext = spark.sqlContext

  test("rich test") {

    val kafkaData = MemoryStream[Event]

    val testDF = kafkaData.toDF()

    val result = richDataFrame(testDF).writeStream
      .format("memory")
      .queryName("rich_table")
      .outputMode("append")
      .start()
    val data1 = Seq(Event("PICKUP"), Event("DROPOFF"))

    kafkaData.addData(data1)
    result.processAllAvailable()

    val expected = List(Row(1, 1), Row(0, 1))

    val collect = spark.sql("select demand, supply from rich_table").collect()
    assert(expected == collect.toList)

  }

  test("End-End streaming test") {

    val testSchema = Encoders.product[order].schema
    val kafkaData = MemoryStream[String]
    val kafkaDF = kafkaData.toDF()

    val testDF = kafkaDF
      .select(from_json($"value".cast("string"), testSchema) as "record")
      .select("record.*")

    val result = ratioDataFrame(richDataFrame(testDF)).writeStream
      .format("memory")
      .queryName("streaming")
      .outputMode("append")
      .start()

    val df = spark.read
      .schema(testSchema)
      .option("header", "true")
      .csv("src/resource/sample-data/yellow_2020.csv")
      .toJSON
    val data = df.collect()

    kafkaData.addData(data)
    result.processAllAvailable()

    val collect = spark.sql("select * from streaming")
    collect.show()

  }

}
