package sensorstatistics

import java.io.File

import scala.collection.immutable.ListMap
import scala.io.Source
import scala.util.Try

case class StatisticsReport(numOfFiles: Int,
                            numOfMeasurements: Int,
                            numOfFailedMeasurements: Int,
                            sensors: Map[String, (Int, Int, Int)],
                            failedSensors: List[String])

object Printer {

  def print(report: StatisticsReport): Unit = {
    println(s"Num of processed files: ${report.numOfFiles}")
    println(s"Num of processed measurements: ${report.numOfMeasurements}")
    println(s"Num of failed measurements: ${report.numOfFailedMeasurements}")
    println()
    println("Sensors with highest avg humidity:")
    println()
    println("sensorMeasurement-id,min,avg,max:")
    report.sensors.foreach(t => println(s"${t._1},${t._2._1},${t._2._3},${t._2._2}"))
    report.failedSensors.foreach(e => println(s"$e,NaN,NaN,NaN"))
  }
}

object StatisticsCalculator {

  def calculateStatistics(dir: File): StatisticsReport = {

    val csvFiles = dir.listFiles().view
      .filter(_.isFile)
      .filter(_.getName.toLowerCase.endsWith(".csv"))

    val partitions = csvFiles.view
      .flatMap(Source.fromFile(_).getLines().drop(1)).map(_.split(",")).filter(_ (0).trim.nonEmpty)
      .map(arr => (arr(0), Try(arr(1).toInt).toOption))
      .partition(_._2.isDefined)

    val successStats = partitions._1.groupBy(_._1).mapValues(_.map(t => (t._2.get, t._2.get, t._2.get, 1))
      .reduce((t1, t2) => (t1._1.min(t2._1), t1._2.max(t2._2), t1._3 + t2._3, t1._4 + t2._4)))

    val failureStats = partitions._2.groupBy(_._1).mapValues(_.count(_._2.isEmpty))

    val failedSensors = failureStats.seq.view.map(_._1).filter(!successStats.contains(_)).toList

    val numOfSuccessfulMeasurements = successStats.seq.view.map(_._2._4).sum
    val numOfFailedMeasurements = failureStats.seq.view.map(_._2).sum
    val numOfMeasurements = numOfSuccessfulMeasurements + numOfFailedMeasurements

    val sensors = ListMap(successStats.mapValues(v => (v._1, v._2, v._3 / v._4)).toSeq.sortWith(_._2._3 > _._2._3): _*)

    StatisticsReport(csvFiles.length, numOfMeasurements, numOfFailedMeasurements, sensors, failedSensors)
  }
}

object SensorStatisticsApp extends App {

  if (args.length != 1) throw new IllegalArgumentException("Application expects exactly one 'path to directory' argument to be passed")

  val file = Some(new File(args(0))).filter(_.isDirectory).getOrElse(throw new IllegalArgumentException(s"'${args(0)}' is not a directory"))

  val report = StatisticsCalculator.calculateStatistics(file)

  Printer.print(report)

}
