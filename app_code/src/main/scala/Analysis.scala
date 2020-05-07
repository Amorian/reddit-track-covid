import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object Analysis
{
    // List of States
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
        // SparkSession
        val spark = SparkSession.builder().appName("Analysis").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        // Mentions in posts
        mentions(spark, Seq("Reddit_Cleaning/posts/*.csv"), "Reddit_Mentions/posts")

        // Mentions in Comments
        mentions(spark, Seq("Reddit_Cleaning/comments/*.csv"), "Reddit_Mentions/comments")

        // Mentions in Posts and Comments
        mentions(spark, Seq("Reddit_Cleaning/posts/*.csv", "Reddit_Cleaning/comments/*.csv"), "Reddit_Mentions/posts_comments")

        // Transform dataset into correct format and normalize columns
        states_transform(spark)

        // Find correlations
        correlations(spark)

        // Summarization of mentions, cases and deaths
        summarization(spark)
    }

    // Find the number of mentions of states
    def mentions(spark: SparkSession, input: Seq[String], output: String)
    {
        // Read and cache due to multiple operations
        var df = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(input: _*).cache()

        // Convert to date
        df = df.withColumn("created_utc", to_date(col("created_utc"), "yyyy-MM-dd"))

        // Count if state exists
        df = states.foldLeft(df) {
          case(temp, state) => temp.withColumn(state, when(lower(col("selftext")).contains(state.toLowerCase), 1).otherwise(0))
        }

        // Group by Time and sum up the state columns
        df = df.groupBy(col("created_utc")).sum(states:_*)

        // Fix column names
        df = df.columns.foldLeft(df) {
          case(temp, column) =>
          if(column != "created_utc")
              temp.withColumnRenamed(column, column.slice(4, column.length - 1))
          else
              temp
        }

        // Sort by date
        df = df.orderBy(col("created_utc"))

        // Print to HDFS in CSV
        df.coalesce(1).write.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv(output)
    }

    // Transform all data to data per state and normalize columns
    def states_transform(spark: SparkSession)
    {
        // Column Names
        val columns = List("Date", "Posts", "Comments", "Posts_Comments", "Cases", "Deaths")

        // Read files in separate dataframes
        var posts = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("Reddit_Mentions/posts/*.csv").cache()
        var comments = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("Reddit_Mentions/comments/*.csv").cache()
        var posts_comments = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("Reddit_Mentions/posts_comments/*.csv").cache()
        var cases = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("NYTimes_Cleaning/cases/*.csv").cache()
        var deaths = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("NYTimes_Cleaning/deaths/*.csv").cache()

        // Transform all data to one dataframe per state
        import spark.implicits._
        for(state <- states)
        {
            // Get each column in a separate variable by making it an RDD and transposing
            val dates = cases.select("date").rdd.map(r => r(0)).collect.toList.map(_.toString)
            val post_list = posts.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)
            val comment_list = comments.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)
            val pc_list = posts_comments.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)
            val case_list = cases.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)
            val death_list = deaths.select(state).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt)

            // Zip all the columns and create dataframe
            var df = spark.sparkContext.parallelize(dates zip post_list zip comment_list zip pc_list zip case_list zip death_list map {case (((((a, b), c), d), e), f) => (a, b, c, d, e, f)}).toDF(columns: _*)

            // Write state data to HDFS in CSV
            df.coalesce(1).write.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("States/" + state)

            // Normalize columns that are not date
            for(column <- columns.slice(1, columns.length))
            {
                val (min_col, max_col) = df.select(min(col(column)), max(col(column))).as[(Double, Double)].first()
                if(max_col != 0)
                    df = df.withColumn(column, (col(column) - min_col) / max_col)
            }

            // Write normalized state data to HDFS in CSV
            df.coalesce(1).write.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("States_Normalized/" + state)

            // To save to hive table -
            // df.write.mode(SaveMode.Overwrite).saveAsTable(sys.env("USER") + "." + state.replaceAll("\\s+", "_").toLowerCase)
        }
    }

    // Find Correlations and make a CSV of states as rows and different combinations of (Posts, Comments, Posts_Comments) vs (Cases, Deaths) as columns
    def correlations(spark: SparkSession)
    {
        // Temp list buffers for columns
        var posts_cases = new ListBuffer[Double]()
        var posts_deaths = new ListBuffer[Double]()
        var comments_cases = new ListBuffer[Double]()
        var comments_deaths = new ListBuffer[Double]()
        var pc_cases = new ListBuffer[Double]()
        var pc_deaths = new ListBuffer[Double]()

        // Per state (row wise)
        import spark.implicits._
        for(state <- states)
        {
            // Read state data
            var df_state = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("States/" + state + "/*.csv")

            // Compute correlations
            posts_cases += df_state.stat.corr("Posts", "Cases")
            posts_deaths += df_state.stat.corr("Posts", "Deaths")
            comments_cases += df_state.stat.corr("Comments", "Cases")
            comments_deaths += df_state.stat.corr("Comments", "Deaths")
            pc_cases += df_state.stat.corr("Posts_Comments", "Cases")
            pc_deaths += df_state.stat.corr("Posts_Comments", "Deaths")
        }

        // Zip all columns and store as dataframe
        val df_corr = spark.sparkContext.parallelize(states zip posts_cases.toList zip posts_deaths.toList zip comments_cases.toList zip comments_deaths.toList zip pc_cases.toList zip pc_deaths.toList map {case ((((((a, b), c), d), e), f), g) => (a, b, c, d, e, f, g)}).toDF("State", "Posts_Cases", "Posts_Deaths", "Comments_Cases", "Comments_Deaths", "Posts_Comments_Cases", "Posts_Comments_Deaths")

        // Write Dataframe to HDFS as CSV
        df_corr.coalesce(1).write.option("header", true).option("multiLine",true).option("inferSchema", "true").csv("Correlations")

        // To save to hive table -
        // df_corr.write.mode(SaveMode.Overwrite).saveAsTable(sys.env("USER") + ".correlations")
    }

    // Summary of mentions, cases and deaths
    def summarization(spark: SparkSession)
    {
        // Temp list buffers for columns
        var totals = new ListBuffer[Double]()
        var measure = new ListBuffer[String]()

        // Finding total number of mentions across states
        measure += "Mentions"
        var posts_comments = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("Reddit_Mentions/posts_comments/*.csv").cache()
        var counter: Long = 0

        // Get sum of number of mentions for each state
        states.foreach(state => counter += posts_comments.agg(sum(state).cast("long")).first.getLong(0))
        totals += counter

        // Finding total number of cases across states
        measure += "Cases"
        counter = 0
        var cases = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("NYTimes_Cleaning/cases/*.csv").cache()

        // Get sum of last element of each state (since number of cases at the moment is always a running sum)
        states.foreach(counter += cases.select(_).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt).last)
        totals += counter

        // Finding total number of deaths across states
        measure += "Deaths"
        counter = 0
        var deaths = spark.read.option("header", true).option("multiLine",true).option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd").csv("NYTimes_Cleaning/deaths/*.csv").cache()

        // Get sum of last element of each state (since number of deaths at the moment is always a running sum)
        states.foreach(counter += deaths.select(_).rdd.map(r => r(0)).collect.toList.map(_.toString.toInt).last)
        totals += counter

        import spark.implicits._
        // Make dataframe by zipping the lists
        val df = spark.sparkContext.parallelize(measure.toList zip totals.toList).toDF("Measure", "Totals")

        // Write dataframe to csv
        df.coalesce(1).write.option("header", true).option("multiLine",true).option("inferSchema", "true").csv("Summarization")
    }
}
