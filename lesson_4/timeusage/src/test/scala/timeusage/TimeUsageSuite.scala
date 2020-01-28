package timeusage

import org.apache.spark.sql.{Column, Dataset, Row}
import org.junit.Assert.assertEquals
import org.junit.Test

class TimeUsageSuite {

  import TimeUsage.spark.implicits._

  @Test def row(): Unit ={
    val line = List("foo", "1", "2")
    val result = TimeUsage.row(line)
    val expected = Row(line:_*)
    assertEquals(expected, result)
  }

  @Test def classifiedColumns(): Unit = {
    val columns = List("tucaseid", "gemetsta", "gtmetsta", "peeduca", "pehspnon", "", "t01", "t03", "t11", "t1801",
      "t1803", "t05", "t1805", "t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16"
      , "t18", "")
    val resultList = TimeUsage.classifiedColumns(columns)
    val result = (resultList._1.toSet, resultList._2.toSet, resultList._3.toSet)
    val expected: (Set[Column], Set[Column], Set[Column]) =
      (
        List($"t01", $"t03", $"t11", $"t1801", $"t1803").toSet
        , List($"t05", $"t1805").toSet
        , List($"t02", $"t04", $"t06", $"t07", $"t08", $"t09", $"t10", $"t12", $"t13", $"t14", $"t15", $"t16", $"t18").toSet
      )
    assertEquals(expected, result)
    assertEquals((Seq(),Seq(),Seq()), TimeUsage.classifiedColumns(Nil))
  }

//  @Test def timeUsageSummary(): Unit = {
//    val df: Dataset[Row] = TimeUsage.spark.createDataset(Seq(
//      Row("2", "", "", 1, 1, 1),
//      Row("", "", "", 1, 1, 1),
//      Row("", "", "", 1, 1, 1)))
//
//    val primaryColumns = List($"primaryNeeds")
//    val workColumns = List($"primaryNeeds")
//    val otherColumns = List($"primaryNeeds")
//
//    val result = TimeUsage.timeUsageSummary(primaryColumns, workColumns, otherColumns, df)
//    val expected = Seq(
//      TimeUsageRow("", "", "", 1, 1, 1),
//      TimeUsageRow("", "", "", 1, 1, 1),
//      TimeUsageRow("", "", "", 1, 1, 1)
//    ).toDF()
//
//    assertEquals(expected, result)
//  }
}