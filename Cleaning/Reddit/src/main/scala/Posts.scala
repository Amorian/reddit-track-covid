import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

object Reddit_Posts_Cleaning
{
    def main(args: Array[String])
    {
        val subreddits = List("Coronavirus", "COVID", "COVID19", "CoronavirusUS", "nCoV", "COVID19_support", "China_Flu")
        val removeCols = Seq("all_awardings", "allow_live_comments", "author_flair_css_class", "author_flair_richtext", "author_flair_text", "author_flair_type", "author_fullname", "author_patreon_flair", "author_premium", "awarders", "can_mod_post", "contest_mode", "domain", "full_link", "gildings", "is_crosspostable", "is_meta", "is_original_content", "is_reddit_media_domain", "is_robot_indexable", "is_self", "is_video", "link_flair_background_color", "link_flair_richtext", "link_flair_template_id", "link_flair_text_color", "link_flair_type", "locked", "media_only", "no_follow", "num_crossposts", "over_18", "permalink", "pinned", "retrieved_on", "score", "send_replies", "spoiler", "stickied", "subreddit_id", "subreddit_subscribers", "subreddit_type", "suggested_sort", "thumbnail", "total_awards_received", "url", "created", "post_hint", "preview", "media", "media_embed", "secure_media", "secure_media_embed", "thumbnail_height", "thumbnail_width", "removed_by_category", "author_cakeday", "link_flair_css_class", "crosspost_parent", "crosspost_parent_list", "author_flair_background_color", "author_flair_template_id", "author_flair_text_color", "media_metadata", "distinguished")
        val conf = new SparkConf().setAppName("Reddit_Posts_Cleaning")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().appName("Reddit_Posts_Cleaning").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        for(sub <- subreddits)
        {
            var df = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("ProjectData/" + sub + "/posts/*.csv")
            df = df.drop(removeCols:_*)
            df = df.na.drop()
            df.write.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode(SaveMode.Append).csv("Reddit_Cleaning/posts/")
        }
    }
}
