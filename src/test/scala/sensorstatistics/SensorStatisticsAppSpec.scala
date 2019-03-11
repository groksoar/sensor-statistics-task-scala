package sensorstatistics

import java.nio.file.{Files, Path}
import java.util.Comparator

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._

class SensorStatisticsAppSpec extends FunSuite with BeforeAndAfterAll {

  var tmpDir: Path = _

  test("statistics should be calculated based on the measurements stored in files") {
    val report = StatisticsCalculator.calculateStatistics(tmpDir.toFile)

    assert(2 == report.numOfFiles)
    assert(7 == report.numOfMeasurements)
    assert(2 == report.numOfFailedMeasurements)

    assert(2 == report.sensors.size)
    assert("s2" == report.sensors.head._1)

    assert(1 == report.failedSensors.size)
    assert("s3" == report.failedSensors.head)
  }

  override protected def beforeAll(): Unit = {
    tmpDir = Files.createTempDirectory("sensors")

    val file1 = Files.createTempFile(tmpDir, "leader-1", ".csv")
    val content1 = List("sensorMeasurement-id,humidity", "s1,10", "s2,88", "s1,NaN").asJava
    Files.write(file1, content1)

    val file2 = Files.createTempFile(tmpDir, "leader-2", ".csv")
    val content2 = List("sensorMeasurement-id,humidity", "s2,80", "s3,NaN", "s2,78", "s1,98").asJava
    Files.write(file2, content2)
  }

  override protected def afterAll(): Unit = Files.walk(tmpDir).sorted(Comparator.reverseOrder()).forEach(_.toFile.delete)
}
