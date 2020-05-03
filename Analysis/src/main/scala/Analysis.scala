import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object Analysis
{
    val states = List("Alaska",
              "Alabama",
              "Arkansas",
              "American Samoa",
              "Arizona",
              "California",
              "Colorado",
              "Connecticut",
              "District of Columbia",
              "Delaware",
              "Florida",
              "Georgia",
              "Guam",
              "Hawaii",
              "Iowa",
              "Idaho",
              "Illinois",
              "Indiana",
              "Kansas",
              "Kentucky",
              "Louisiana",
              "Massachusetts",
              "Maryland",
              "Maine",
              "Michigan",
              "Minnesota",
              "Missouri",
              "Mississippi",
              "Montana",
              "North Carolina",
              "North Dakota",
              "Nebraska",
              "New Hampshire",
              "New Jersey",
              "New Mexico",
              "Nevada",
              "New York",
              "Northern Mariana Islands",
              "Ohio",
              "Oklahoma",
              "Oregon",
              "Pennsylvania",
              "Puerto Rico",
              "Rhode Island",
              "South Carolina",
              "South Dakota",
              "Tennessee",
              "Texas",
              "Utah",
              "Virginia",
              "Virgin Islands",
              "Vermont",
              "Washington",
              "Wisconsin",
              "West Virginia",
              "Wyoming")

    def main(args: Array[String])
    {
        val spark = SparkSession.builder().appName("Analysis").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        mentions(spark, Seq("Reddit_Cleaning/posts/*.csv"), "Reddit_Mentions/posts")
        mentions(spark, Seq("Reddit_Cleaning/comments/*.csv"), "Reddit_Mentions/comments")
        mentions(spark, Seq("Reddit_Cleaning/posts/*.csv", "Reddit_Cleaning/comments/*.csv"), "Reddit_Mentions/posts_comments")
        states_transform(spark)
        correlations(spark)
    }

    def mentions(spark: SparkSession, input: Seq[String], output: String)
    {
        var df = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(input: _*)

        df = df.withColumn("created_utc", to_date(col("created_utc"), "yyyy-MM-dd"))

        df = states.foldLeft(df) {
          case(temp, state) => temp.withColumn(state, when(lower(col("selftext")).contains(state.toLowerCase), 1).otherwise(0))
        }

        df = df.groupBy(col("created_utc")).sum(states:_*)

        df = df.columns.foldLeft(df) {
          case(temp, column) =>
          if(column != "created_utc")
              temp.withColumnRenamed(column, column.slice(4, column.length - 1))
          else
              temp
        }

        df = df.orderBy(col("created_utc"))

        df.coalesce(1).write.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv(output)
    }

    def states_transform(spark: SparkSession)
    {
        var posts = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("Reddit_Mentions/posts/*.csv")
        var comments = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("Reddit_Mentions/comments/*.csv")
        var posts_comments = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("Reddit_Mentions/posts_comments/*.csv")
        var cases = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("NYTimes_Cleaning/cases/*.csv")
        var deaths = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("NYTimes_Cleaning/deaths/*.csv")

        import spark.implicits._
        for(state <- states)
        {
            val dates = cases.select("date").rdd.map(r => r(0)).collect.toList.map(_.toString)
            val post_list = posts.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)
            val comment_list = comments.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)
            val pc_list = posts_comments.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)
            val case_list = cases.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)
            val death_list = deaths.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)
            val df = spark.sparkContext.parallelize(dates zip post_list zip comment_list zip pc_list zip case_list zip death_list map {case (((((a, b), c), d), e), f) => (a, b, c, d, e, f)}).toDF("Date", "Posts", "Comments", "Posts_Comments", "Cases", "Deaths")
            df.coalesce(1).write.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("States/" + state)
        }
    }

    def correlations(spark: SparkSession)
    {
        var posts_cases = new ListBuffer[Double]()
        var posts_deaths = new ListBuffer[Double]()
        var comments_cases = new ListBuffer[Double]()
        var comments_deaths = new ListBuffer[Double]()
        var pc_cases = new ListBuffer[Double]()
        var pc_deaths = new ListBuffer[Double]()

        import spark.implicits._
        for(state <- states)
        {
            var df_state = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("States/" + state + "/*.csv")
            posts_cases += df_state.stat.corr("Posts", "Cases")
            posts_deaths += df_state.stat.corr("Posts", "Deaths")
            comments_cases += df_state.stat.corr("Comments", "Cases")
            comments_deaths += df_state.stat.corr("Comments", "Deaths")
            pc_cases += df_state.stat.corr("Posts_Comments", "Cases")
            pc_deaths += df_state.stat.corr("Posts_Comments", "Deaths")
        }
        val df_corr = spark.sparkContext.parallelize(states zip posts_cases.toList zip posts_deaths.toList zip comments_cases.toList zip comments_deaths.toList zip pc_cases.toList zip pc_deaths.toList map {case ((((((a, b), c), d), e), f), g) => (a, b, c, d, e, f, g)}).toDF("State", "Posts_Cases", "Posts_Deaths", "Comments_Cases", "Comments_Deaths", "Posts_Comments_Cases", "Posts_Comments_Deaths")
        df_corr.coalesce(1).write.option("header", true).option("multiLine",true).option("inferSchema", "true").csv("Correlations")
    }
}
