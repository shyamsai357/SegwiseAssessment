package org.segwise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, count, dense_rank, desc, lit, rank, sum, udf}

object Segwise {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Segwise")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/input/records1000")
    // orignal dataset not commited to git due to size
    //Note: All data generated is with the help of original dataset
    //.csv("src/main/resources/input/original/google-play-dataset-by-tapivedotcom.csv")

    df.show()
    df.printSchema()

    /*
    df schema is as below:
    root
      |-- _c0: string (nullable = true)
      |-- appId: string (nullable = true)
      |-- developer: string (nullable = true)
      |-- developerId: string (nullable = true)
      |-- developerWebsite: string (nullable = true)
      |-- free: string (nullable = true)
      |-- genre: string (nullable = true)
      |-- genreId: string (nullable = true)
      |-- inAppProductPrice: string (nullable = true)
      |-- minInstalls: string (nullable = true)
      |-- offersIAP: string (nullable = true)
      |-- originalPrice: string (nullable = true)
      |-- price: string (nullable = true)
      |-- ratings: string (nullable = true)
      |-- len screenshots: string (nullable = true)
      |-- adSupported: string (nullable = true)
      |-- containsAds: string (nullable = true)
      |-- reviews: string (nullable = true)
      |-- releasedDayYear: string (nullable = true)
      |-- sale: string (nullable = true)
      |-- score: string (nullable = true)
      |-- summary: string (nullable = true)
      |-- title: string (nullable = true)
      |-- updated: string (nullable = true)
      |-- histogram1: string (nullable = true)
      |-- histogram2: string (nullable = true)
      |-- histogram3: string (nullable = true)
      |-- histogram4: string (nullable = true)
      |-- histogram5: string (nullable = true)
      |-- releasedDay: string (nullable = true)
      |-- releasedYear: string (nullable = true)
      |-- releasedMonth: string (nullable = true)
      |-- dateUpdated: string (nullable = true)
      |-- minprice: string (nullable = true)
      |-- maxprice: string (nullable = true)
      |-- ParseReleasedDayYear: string (nullable = true)
     */


    //Cast ratings column to integer
    val df1 = df.withColumn("rating", $"ratings".cast("Int"))


    //Q1.How many apps which are free and for a certain genre and launched in a particular year etc?

    // df2 --> Top Free apps for ART_AND_DESIGN genre in 2018 descending order by ratings
    val df2 = df1.select("title", "releasedYear", "free", "genreId", "ratings").where("free = 1 AND genreId = 'ART_AND_DESIGN' AND releasedYear = 2018").sort(desc("rating"))
    println("Top Free apps for ART_AND_DESIGN genre in 2018 descending order by ratings")
    df2.show(false)
    /*
      +-----------------------------------------------------------+------------+----+--------------+-------+
      |title                                                      |releasedYear|free|genreId       |ratings|
      +-----------------------------------------------------------+------------+----+--------------+-------+
      |Thumbnail Maker - Channel art                              |2018        |1   |ART_AND_DESIGN|149626 |
      |Adobe Express: Graphic Design                              |2018        |1   |ART_AND_DESIGN|126331 |
      |Text on Photo - Text to Photos                             |2018        |1   |ART_AND_DESIGN|103510 |
      |Unwanted Object Remover - Remo                             |2018        |1   |ART_AND_DESIGN|102748 |
      |X Launcher: With OS13 Theme                                |2018        |1   |ART_AND_DESIGN|95020  |
      |Easy Pose - 3D pose making app                             |2018        |1   |ART_AND_DESIGN|89525  |
      |Anime Wallpaper HD | 4K List                               |2018        |1   |ART_AND_DESIGN|71595  |
      |Poster Maker, Flyer Maker                                  |2018        |1   |ART_AND_DESIGN|69814  |
     */


    // df3 --> Top 3 free app in each genreId descending order by ratings
    val df3 = df1.select("title", "genreId", "rating").where("free = 1").withColumn("rank", rank().over(Window.partitionBy("genreId").orderBy($"rating".desc))).filter($"rank" <= 3).drop("rank")
    println("Top 3 free app in each genreId descending order by ratings")
    df3.show(false)
    /*
    +-----------------------------------------------------------------+-------------------+---------+
    |title                                                            |genreId            |rating   |
    +-----------------------------------------------------------------+-------------------+---------+
    |Ticketmaster?Buy, Sell Tickets                                  |EVENTS             |111327   |
    |Invitation maker & Card design                                   |EVENTS             |107255   |
    |StubHub - Live Event Tickets                                     |EVENTS             |75082    |
    |WEBTOON                                                          |COMICS             |2809227  |
    |MangaToon - Manga Reader                                         |COMICS             |620418   |
    |Translator Women's Voice - TTS                                   |COMICS             |460783   |
    |Cricbuzz - Live Cricket Scores                                   |SPORTS             |1557176  |
    |OneFootball - Soccer Scores                                      |SPORTS             |1536446  |
     */

    //Q2.How many apps between certain price range, released in certain year, is adSupported and has a certain ratings and of a certain price etc?

    // df4 --> Apps between price range 1 and 100, released in 2018, is adSupported and has a rating of 10000 and above
    val df4 = df1.select("title", "price", "releasedYear", "adSupported", "rating").where("price > 0 AND price <= 100 AND releasedYear = 2018 AND adSupported = 1 AND rating >= 10000").sort(desc("rating"))
    println("Apps between price range 1 and 100, released in 2018, is adSupported and has a rating of 10000 and above")
    df4.show(false)
    /*
      +-----------------------------+-----+------------+-----------+------+
      |title                        |price|releasedYear|adSupported|rating|
      +-----------------------------+-----+------------+-----------+------+
      |Shadow of Death: Dark Knight |3.99 |2018        |1          |457652|
      |Cartoon Craft                |1.99 |2018        |1          |65613 |
      |null                         |3.99 |2018        |1          |33680 |
      |Dungeon Shooter : Dark Temple|3.49 |2018        |1          |21047 |
      |Teen Titans GO Figure!       |3.99 |2018        |1          |18220 |
      +-----------------------------+-----+------------+-----------+------+
     */


    //Q3. Year=[2005-2010], 1000
    // This means there are a 1000 apps released between years 2005-2010.

    val df6 = df1.select("appId", "releasedYear").withColumn("releaseYear", $"releasedYear".cast("Int")).drop("releasedYear")

    // Define the minimum count for a bin to be considered
    val minCount = 20

    // UDF to create a bin for the release year
    val createYearBin = udf((year: Int) => s"${(year / 5) * 5}-${(year / 5) * 5 + 4}")

    // Create bins and calculate counts
    val binsDF = df6
      .withColumn("bin", createYearBin(col("releaseYear")))
      .groupBy("bin", "releaseYear")
      .agg(count("*").as("count"))
      .filter(col("count") >= minCount)

    // Window specification for ranking
    val windowSpec = Window.orderBy(col("count").desc)

    // Rank the bins based on count
    val rankedBinsDF = binsDF.withColumn("rank", dense_rank().over(windowSpec))

    // Filter out bins that contribute less than 2% of the total volume
    val totalVolume = rankedBinsDF.agg(sum("count")).collect()(0).getLong(0)
    val filteredBinsDF = rankedBinsDF.filter(col("count") >= totalVolume * 0.02)

    // Join the filtered bins with the original DataFrame to get other columns
    val resultDF = df6.select("releaseYear").distinct()
      .join(filteredBinsDF, df6("releaseYear") === filteredBinsDF("releaseYear"))
      .select(
        concat(lit("Year=["), col("bin"), lit("]")).as("year"),
        col("count")
      )

    println("Data like --> Year=[2005-2010], 1000")
    resultDF.show(truncate = false)
    /*
      +----------------+------+
      |year            |count |
      +----------------+------+
      |Year=[2015-2019]|298600|
      |Year=[2015-2019]|104936|
      |Year=[2020-2024]|586160|
      |Year=[2010-2014]|72401 |
      |Year=[2015-2019]|514839|
      |Year=[2020-2024]|696791|
      |Year=[2015-2019]|145384|
      |Year=[2015-2019]|218256|
      |Year=[2020-2024]|716182|
      +----------------+------+
     */

    //save resultsDf in overwrite mode
    resultDF.coalesce(1).write.mode("overwrite").csv("src/main/resources/output/YearRange5")

    /*
    println(resultDF.agg(sum("count")).collect()(0).getLong(0)) //3353549 -- count decreased from 3460966 to 3353549 after filtering out bins that contribute less than 2% of the total volume
    println(df6.count()) //3460966
     */

    //Q4. Price=[4-5]; genre=Art & Design; Installs=[10000-100000], 100
    //This means that there are a 100 apps with a combination of price between 4-5, belonging to genre “Art & Design”, installs
    //between 10k and 100k.
    val df7 = df1.select("price", "genre", "minInstalls").withColumn("appPrice", $"price".cast("Double"))
      .withColumn("installs", $"minInstalls".cast("Long")).drop("price", "minInstalls")

    /*
    df7.printSchema()
    root
     |-- genre: string (nullable = true)
     |-- appPrice: double (nullable = true)
     |-- installs: long (nullable = true)
     */


    // UDF to create a bin for the price and installs
    val createBin = udf((value: Double, binSize: Double) => {
      val lowerBound = (value / binSize).toInt * binSize
      s"[$lowerBound-${lowerBound + binSize}]"
    })

    // Create bins for price and installs
    val dfBins = df7
      .withColumn("priceBin", createBin(col("appPrice"), lit(1.0)))
      .withColumn("installsBin", createBin(col("installs"), lit(10000.0)))
      .groupBy("priceBin", "genre", "installsBin")
      .agg(count("*").as("count"))
      .filter(col("count") >= minCount)

    println("Data like --> Price=[4-5]; genre=Art & Design; Installs=[10000-100000], 1000")
    dfBins.orderBy("count").show(false)
    /*
      +------------+-----------------+-------------------+-----+
      |priceBin    |genre            |installsBin        |count|
      +------------+-----------------+-------------------+-----+
      |[8.0-9.0]   |Health & Fitness |[0.0-10000.0]      |20   |
      |[2.0-3.0]   |Comics           |[0.0-10000.0]      |20   |
      |[13.0-14.0] |Business         |[0.0-10000.0]      |20   |
      |[9.0-10.0]  |Puzzle           |[0.0-10000.0]      |20   |
     */

    dfBins.coalesce(1).write.mode("overwrite").csv("src/main/resources/output/PriceGenreInstalls")

  }
}
