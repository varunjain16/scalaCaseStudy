import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, explode, regexp_extract, regexp_replace}

object AdidasMain {

  def main(args: Array[String]) : Unit = {

    //Create Spark session
    val sparkConf = new SparkConf().setMaster("local").setAppName("Adidas Case Study")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //Load Data
    val input = sparkSession.read.json("C:/Users/varjain6/data.json")

    /*
    Clean input data based on below assumptions/filters -
      - Records where both Titles and Authors is null are filtered out
      - Titles ending with "=" or having "+" character are ignored
      - Publish year is calculated to process publish date with format e.g.August 23, 2003
      - Publish year >1950 and Number of Pages > 20 is considered
      - Genres is considered an optional field and having null values. Records with null genres is also included */
    val cleanedDf = input.withColumn("author", explode(input.col("authors.key")))
                          .filter(input.col("title").isNotNull && col("author").isNotNull)
                          .filter(input.col("number_of_pages").gt(20))
                          .filter(!input.col("title").rlike("[=$|\\\\+]"))
                          .filter(input.col("publish_date").rlike("2021|20[0-9]{2}|19[0-9]{2}"))
                          .withColumn("publish_year",regexp_extract(input.col("publish_date"),
                                                                "2021|20[0-9]{2}|19[0-9]{2}",0)
                            .cast("Integer")).drop(input.col("publish_date"))
                          .filter(col("publish_year").gt(1950))
                          .select(col("title"), col("author"), col("publish_year"),
                            col("number_of_pages"),col("genres")).distinct()

    //Select all "Harry Potter" books
    val solution1 = cleanedDf.where("title like '%Harry Potter%'")

    println("solution1---"+solution1.show(false))

    //Get the book with the most pages
    val solution2 = cleanedDf.orderBy(cleanedDf.col("number_of_pages").desc).limit(1)

    println("solution2---"+solution2.show(false))


    /*Find the Top 5 authors with most written books (assuming author in first position in the array, "key" field and
       each row is a different book) */
    val solution3 = cleanedDf.groupBy("author").count().orderBy(col("count").desc).limit(5)

    println("solution3---"+solution3.show(false))

    //Find the Top 5 genres with most books
    val solution4 = cleanedDf.withColumn("genre", explode(col("genres")))
                             .withColumn("genre", regexp_replace(col("genre"), "\\.$", ""))
                             .drop("genres").groupBy("genre").count()
                             .orderBy(col("count").desc).limit(5)

    println("solution4---"+solution4.show(false))


    //Get the avg. number of pages
    val solution5 = cleanedDf.agg(avg("number_of_pages").alias("Average_Num_Pages"))

    println("solution5---"+solution5.show(false))


    //Per publish year, get the number of authors that published at least one book
    val solution6 = cleanedDf.groupBy("publish_year", "author").count()
                             .filter("count >=1").drop("count")
                             .groupBy("publish_year").count()
                             .orderBy(col("publish_year").desc)

    println("solution6---"+solution6.show(false))


  }

}
