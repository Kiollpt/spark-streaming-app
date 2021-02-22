import taxiapp.richDataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.control.Exception

trait DataSetCompare {

  def schemaCheck(s1: StructType, s2: StructType): Boolean = {

    if (s1.length != s2.length) {
      false
    } else {
      val fields: Seq[(StructField, StructField)] = s1.zip(s2)

      fields.forall(t => t._1.dataType == t._2.dataType)

    }

  }

  def assertDataSet[T](actualDS: Dataset[T], expectedDS: Dataset[T]): Unit = {

    if (!schemaCheck(actualDS.schema, expectedDS.schema)) {
      throw new Exception("the schema are not matched")
    }

    val actual = actualDS.collect()
    val expected = expectedDS.collect()

    if (!actual.sameElements(expected)) {
      throw new Exception("the data are not matched")
    }

  }

}
