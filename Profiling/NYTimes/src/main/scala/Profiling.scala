import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object NYTimes_Profiling
{
    def main(args: Array[String])
    {
        val conf = new SparkConf().setAppName("NYTimes_Profiling")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().appName("NYTimes_Profiling").getOrCreate()
        var original = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("ProjectData/us-states.csv")
        var clean = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("NYTimes_Cleaning/")
        val c1 = sc.parallelize(Seq("Original count: " + original.count()))
        val c2 = sc.parallelize(Seq("Cleaned count: " + clean.count()))
        val count = c1 ++ c2
        count.saveAsTextFile("NYTimes_Profiling/")
    }
}
