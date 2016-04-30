import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.joda.time.DateTimeConstants


object SundayCount {
  def main(args: Array[String]): Unit = {
    if(args.length < 1) throw new IllegalArgumentException("Input File Path!")

    val filePath = args(0)
    val conf = new SparkConf
    val sc = new SparkContext(conf)

    try{
      val textRDD = sc.textFile(filePath)

      val dataTimeRDD = textRDD.map { dateStr =>
        val pattern = DateTimeFormat.forPattern("yyyyMMdd")
        DateTime.parse(dateStr, pattern)
      }

      val sundayRDD = dataTimeRDD.filter { dataTime =>
        dataTime.getDayOfWeek == DateTimeConstants.SUNDAY
      }

      val numberOfSunday = sundayRDD.count
      println(s"Number Of Sunday = ${numberOfSunday}.")

    }finally {
      sc.stop()
    }
  }
}