import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object NYTimes_Cleaning
{
    def main(args: Array[String])
    {
        val conf = new SparkConf().setAppName("NYTimes_Cleaning")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().appName("NYTimes_Cleaning").getOrCreate()
        var df = spark.read.option("header", true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("ProjectData/us-states.csv")
        df = df.drop("fips")
        df = df.na.drop()
        var df_cases = df.groupBy("date").pivot("state").sum("cases")
        df_cases = df_cases.na.fill(0)
        df_cases = df_cases.orderBy(col("date"))
        var df_deaths = df.groupBy("date").pivot("state").sum("deaths")
        df_deaths = df_deaths.na.fill(0)
        df_deaths = df_deaths.orderBy(col("date"))
        df_cases.coalesce(1).write.option("header", true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("NYTimes_Cleaning/cases")
        df_deaths.coalesce(1).write.option("header", true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("NYTimes_Cleaning/deaths")
    }
}
