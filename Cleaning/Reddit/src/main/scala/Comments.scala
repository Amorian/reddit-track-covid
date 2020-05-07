import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// App to clean reddit comments
object Reddit_Comments_Cleaning
{
    def main(args: Array[String])
    {
        // Subreddits
        val subreddits = List("Coronavirus", "COVID", "COVID19", "CoronavirusUS", "nCoV", "COVID19_support", "China_Flu")

        // Columns to remove
        val removeCols = Seq("all_awardings", "associated_award", "author_flair_background_color", "author_flair_css_class", "author_flair_richtext", "author_flair_template_id", "author_flair_text", "author_flair_text_color", "author_flair_type", "author_fullname", "author_patreon_flair", "author_premium", "awarders", "collapsed_because_crowd_control", "gildings", "is_submitter", "link_id", "locked", "no_follow", "parent_id", "permalink", "retrieved_on", "score", "send_replies", "stickied", "subreddit_id", "total_awards_received", "created", "distinguished", "author_cakeday", "edited", "treatment_tags")

        // Spark Context
        val spark = SparkSession.builder().appName("Reddit_Comments_Cleaning").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        // For all subreddits, drop unnecessary columns and empty rows
        for(sub <- subreddits)
        {
            // Read comment files
            var df = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("ProjectData/" + sub + "/comments/*.csv")

            // Drop columns
            df = df.drop(removeCols:_*)

            // Drop empty rows
            df = df.na.drop()

            // Reshape data to fit reddit posts (adding some empty columns to fit)
            df = df.select(col("author"), col("created_utc"), col("id"), lit("").as("link_flair_text"), lit(0).as("num_comments"), col("body").as("selftext"), col("subreddit"), lit("").as("title"))

            // Write data back to HDFS in CSV
            df.write.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode(SaveMode.Append).csv("Reddit_Cleaning/comments/")
        }
    }
}
