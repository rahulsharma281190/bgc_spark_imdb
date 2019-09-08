# bgc_spark_imdb
Spark Scala DF API

Attached are two different solutions of the problems. Below is the combined spark scala code for both the problems as well:-

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import spark.implicits._
object IMDBDataAnalyticsDetails {
def main(args: Array[String]){
// Create a spark session
val spark = SparkSession.builder .master("local[*]").appName("BGCExample").getOrCreate()
//Read the CSV file into a DF
val title_basics_df=spark.read.option("delimiter", "\t").option("header","true").option("compression","gzip").csv("/user/sharmar2/title.basics.tsv.gz")
val title_ratings_df=spark.read.option("delimiter", "\t").option("header","true").option("compression","gzip").csv("/user/sharmar2/title.ratings.tsv.gz")
val name_basics_df=spark.read.option("delimiter", "\t").option("header","true").option("compression","gzip").csv("/user/sharmar2/name.basics.tsv.gz")
val title_principals_df=spark.read.option("delimiter", "\t").option("header","true").option("compression","gzip").csv("/user/sharmar2/title.principals.tsv.gz")
//Filter out the DF for the movies titleType
val movieDf=title_basics_df.selectExpr("tconst","lower(titleType) as titleType","primaryTitle","originalTitle").filter($"titleType"==="movie").drop("titleType")
//Find out movies having minimum 50 votes
val titleFilterDF=title_ratings_df.selectExpr("tconst","cast(averageRating as double) as averageRating","cast(numVotes as double) as numVotes").filter($"numVotes">50)
//Find average no.of votes
val avg_num_votes=titleFilterDF.selectExpr("avg(numVotes) as avgVotes").rdd.map(x=>x.get(0).toString()).collect()(0)
//Applying the mathematical function
val derived_df=titleFilterDF.join(movieDf,Seq("tconst")).withColumn("derived",($"numVotes"/lit(avg_num_votes))*$"averageRating")
//Create window function over this
val myWindow = Window.orderBy($"derived".desc)
//Apply window function over the derived df
val ranked_df = derived_df.withColumn("Rank",row_number().over(myWindow))
//Find out the top 20 movies
val top_20_movie_df = ranked_df.filter($"Rank"<=20)
//Save the result dataframe into a csv
top_20_movie_df.coalesce(1).write.csv("/user/sharmar2/top_20_movies.csv")

//Join the 3 dataframes
val joined_df = top_20_movie_df.join(title_principals_df,Seq("tconst")).join(name_basics_df,Seq("nconst"))
//Output the results of the df to csv
val result_df = joined_df.select($"primaryTitle",$"primaryName",$"knownForTitles").distinct()
//Save the result Dataframes to a csv
result_df.coalesce(1).write.csv("/user/sharmar2/names_titles.csv")
}}
