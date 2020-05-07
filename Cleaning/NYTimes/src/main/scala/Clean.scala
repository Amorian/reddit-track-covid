import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// App to clean NYTimes Data
object NYTimes_Cleaning
{
    def main(args: Array[String])
    {
        // Spark Context
        val spark = SparkSession.builder().appName("NYTimes_Cleaning").getOrCreate()

        // Read input csv
        var df = spark.read.option("header", true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("ProjectData/us-states.csv")

        // Drop useless column
        df = df.drop("fips")

        // Drop empty rows
        df = df.na.drop()

        // Aggregate cases by state and reshape
        var df_cases = df.groupBy("date").pivot("state").sum("cases")

        // Fill empty if necessary
        df_cases = df_cases.na.fill(0)

        // Reorder data for date order
        df_cases = df_cases.orderBy(col("date"))

        // Aggregate deaths by state and reshape
        var df_deaths = df.groupBy("date").pivot("state").sum("deaths")

        // Fill empty if necessary
        df_deaths = df_deaths.na.fill(0)

        // Reorder data for date order
        df_deaths = df_deaths.orderBy(col("date"))

        // Write files to HDFS in CSV
        df_cases.coalesce(1).write.option("header", true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("NYTimes_Cleaning/cases")
        df_deaths.coalesce(1).write.option("header", true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("NYTimes_Cleaning/deaths")
    }
}
