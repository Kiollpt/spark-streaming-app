import org.apache.spark.sql.{SparkSession, SQLContext}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.Row

import taxiapp.{withSupplyDemand, ratioDataFrame}
case class Event(event: String)

class firstTest extends AnyFunSuite with DataSetCompare {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("test")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._
  implicit val sqlCtx: SQLContext = spark.sqlContext

  test("rich test") {

    val kafkaDataMock = MemoryStream[Event]

    val kafkaDF = kafkaDataMock.toDF()

    val query = kafkaDF
      .transform(withSupplyDemand())
      .writeStream
      .format("memory")
      .queryName("rich_table")
      .outputMode("append")
      .start()

    val events = Seq(Event("PICKUP"), Event("DROPOFF"))

    kafkaDataMock.addData(events)
    query.processAllAvailable()

    val expected = List(Row(1, 1), Row(0, 1))

    val q = spark.sql("SELECT demand, supply FROM rich_table").collect()
    assert(expected == q.toList)

    query.stop()

  }

  test("End-End streaming test") {

    val testSchema = Encoders.product[Taxi].schema
    val kafkaDataMock = MemoryStream[String]
    val kafkaDF = kafkaDataMock.toDF()

    val taxiDF = kafkaDF
      .select(from_json($"value".cast("string"), testSchema) as "record")
      .select("record.*")

    val richDF = taxiDF.transform(withSupplyDemand())

    val query = ratioDataFrame(richDF).writeStream
      .format("memory")
      .queryName("streaming")
      .outputMode("append")
      .start()

    val csvDF = spark.read
      .schema(testSchema)
      .option("header", "true")
      .csv("src/resource/sample-data/yellow_2020.csv")
      .toJSON

    kafkaDataMock.addData(csvDF.collect())
    query.processAllAvailable()

    val q = spark.sql("SELECT COUNT(*) FROM streaming").collect()
    val totalCount = q(0).getLong(0)

    assert(totalCount == 23)
    query.stop()

  }

}
