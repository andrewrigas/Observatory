package observatory

import java.time.LocalDate
import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
  * 1st milestone: data extraction
  */
object Extraction {


  val spark =  SparkSession.builder()
    .master("Observatory")
    .config("spark.master","local")
    .getOrCreate()


  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */

  import spark.implicits._

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString


  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationsRDD: RDD[String] = spark.sparkContext.textFile(fsPath(stationsFile))
    val temperaturesRDD: RDD[String] = spark.sparkContext.textFile(fsPath(temperaturesFile))

    val stationsSchemaRDD = stationsRDD
      .map(_.split(",",4))
      .filter(line => line(2).nonEmpty && line(3).nonEmpty)
      .map( line => ((line(0),line(1)),Location(line(2).toDouble,line(3).toDouble)))

    val temperaturesSchemaRDD = temperaturesRDD
      .map(_.split(','))
      .filter(_.length == 5)
      .map(line => ((line(0), line(1)), (LocalDate.of(year, line(2).toInt, line(3).toInt), FahrenheitToCelsius(line(4).toDouble))))

    val joinedRDD = stationsSchemaRDD.join(temperaturesSchemaRDD)

    val values = joinedRDD.mapValues(v => (v._2._1, v._1, v._2._2)).values

    values.collect().toSeq
  }


  def FahrenheitToCelsius(f: Double) = {
    ((f - 32) * 5.0 )/ 9.0
  }
  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val recordsRDD = spark.sparkContext.parallelize(records.toSeq).map(x => (x._2,x._3))

    val recordsDS: Dataset[(Location, Temperature)] = recordsRDD.toDS()

    val groupedDS: Dataset[(Location, Temperature)] = recordsDS.groupBy("_1").mean("_2").as[(Location,Temperature)]

    groupedDS.collect().toSeq
  }

}
