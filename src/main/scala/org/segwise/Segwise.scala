package org.segwise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, rank}

object Segwise {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Segwise")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/input/records1000")
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
    df2.show(1000, false)
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
    df3.show(1000, false)
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
    df4.show(1000, false)
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

  }
}
