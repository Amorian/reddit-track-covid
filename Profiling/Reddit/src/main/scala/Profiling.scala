import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

// App to profile reddit data after cleaning
object Reddit_Profiling
{
    def main(args: Array[String])
    {
        // Original files
        val original_list = Seq("ProjectData/Coronavirus/posts/*.csv", "ProjectData/Coronavirus/comments/*.csv", "ProjectData/COVID/posts/*.csv", "ProjectData/COVID/comments/*.csv", "ProjectData/COVID19/posts/*.csv", "ProjectData/COVID19/comments/*.csv", "ProjectData/CoronavirusUS/posts/*.csv", "ProjectData/CoronavirusUS/comments/*.csv", "ProjectData/nCoV/posts/*.csv", "ProjectData/nCoV/comments/*.csv", "ProjectData/COVID19_support/posts/*.csv", "ProjectData/COVID19_support/comments/*.csv", "ProjectData/China_Flu/posts/*.csv", "ProjectData/China_Flu/comments/*.csv")

        // Spark Context
        val spark = SparkSession.builder().appName("Reddit_Profiling").getOrCreate()

        // Read all original files
        var df = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(original_list)

        // Read cleaned files
        var cleaned_df = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(Seq("Reddit_Cleaning/posts/*.csv", "Reddit_Cleaning/comments/*.csv"))

        // Count
        val count1 = sc.parallelize(Seq("Original count: " + df.count()))
        val count2 = sc.parallelize(Seq("Cleaned count: " + cleaned_df.count()))
        val count_combined = count1 ++ count2

        // Save to file
        count_combined.saveAsTextFile("Reddit_Profiling/")
        }
    }
}
