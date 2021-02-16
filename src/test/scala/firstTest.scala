import org.apache.spark.sql.{SparkSession,SQLContext}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.Row

import taxiapp.richDataFrame

class firstTest extends AnyFunSuite with DataSetCompare {

    test("rich test"){
    lazy val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("test")
      .getOrCreate()


      import spark.implicits._
      implicit val sqlCtx: SQLContext = spark.sqlContext

      val testSchema = Encoders.product[order].schema
      val ds = spark.read.format("csv")
        .option("header", "true")
        .schema(testSchema)
        .load("src/resource/yellow_2020.csv").toJSON

      val data = ds.collect()

      val kafkaData = MemoryStream[String]
      val testDF = kafkaData.toDF()

      val testDF1 = testDF.select(from_json($"value".cast("string"), testSchema) as "record").select("record.*")
      val result = richDataFrame(testDF1).writeStream
        .format("memory")
        .queryName("rich_table")
        .outputMode("append")
        .start


      kafkaData.addData(data)
      result.processAllAvailable()

      val expected = List(Row(1,1),Row(0,2))


      val collect = spark.sql("select demand, supply from rich_table LIMIT 2").collect()
      assert(expected==collect.toList)



  }



}
