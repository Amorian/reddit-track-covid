import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

// App to profile NYTimes Data after cleaning
object NYTimes_Profiling
{
    def main(args: Array[String])
    {
        // Spark Context
        val spark = SparkSession.builder().appName("NYTimes_Profiling").getOrCreate()

        // Read original file
        var original = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("ProjectData/us-states.csv")

        // Read cleaned files
        var clean = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv(Seq("NYTimes_Cleaning/cases", "NYTimes_Cleaning/deaths"))

        // Count
        val c1 = sc.parallelize(Seq("Original count: " + original.count()))
        val c2 = sc.parallelize(Seq("Cleaned count: " + clean.count()))
        val count = c1 ++ c2

        // Save to file
        count.saveAsTextFile("NYTimes_Profiling/")
    }
}
