import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object Reddit_Profiling
{
    def main(args: Array[String])
    {
        val conf = new SparkConf().setAppName("Reddit_Profiling")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().appName("Reddit_Profiling").getOrCreate()
        var df = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("ProjectData/Coronavirus/posts/2020-02-02_2020-02-09.csv")
        var cleaned_df = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("Reddit_Cleaning/")
        val count1 = sc.parallelize(Seq("Original count: " + df.count()))
        val count2 = sc.parallelize(Seq("Cleaned count: " + cleaned_df.count()))
        val count_combined = count1 ++ count2
        count_combined.saveAsTextFile("Reddit_Profiling/")
    }
}
