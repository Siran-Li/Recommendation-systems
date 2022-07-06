import org.rogach.scallop._
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import ujson._

package economics {

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

    val price_M7 = 38600.0
    val rate_M7 = 20.4

    val con_per_day_GB = 1.6 * scala.math.pow(10, -7) * 3600 * 24
    val con_per_day_vCPU = 1.14 * scala.math.pow(10, -6) * 3600 * 24

    val RPi_TP_vCPU = 0.25
    val M7_TP_vCPU = 28

    val RPi_RAM_GB = 8
    val M7_RAM_GB = 24 * 64

    val price_RPi = 108.48
    val RPisDailyCostIdle = 3 * 0.25 * 24/1000
    val RPisDailyCostComputing = 4 * 0.25 * 24/1000


    val days_container_like_RPi = con_per_day_GB * RPi_RAM_GB + con_per_day_vCPU * RPi_TP_vCPU

    val MinRentingDaysIdleRPiPower = (price_RPi / (days_container_like_RPi - RPisDailyCostIdle)).ceil
    val MinRentingDaysComputingRPiPower = (price_RPi / (days_container_like_RPi - RPisDailyCostComputing)).ceil

    val NbRPisEqBuyingICCM7 = (price_M7 / price_RPi).floor
    val RatioRAMRPisVsICCM7 = (NbRPisEqBuyingICCM7 * RPi_RAM_GB) / M7_RAM_GB
    val RatioComputeRPisVsICCM7 = (NbRPisEqBuyingICCM7 * RPi_TP_vCPU) / M7_TP_vCPU

    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }
    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {

        val answers = ujson.Obj(
          "E.1" -> ujson.Obj(
            "MinRentingDays" -> (price_M7/rate_M7).ceil // Datatype of answer: Double
          ),
          "E.2" -> ujson.Obj(
            "ContainerDailyCost" -> 4 * days_container_like_RPi,
            "4RPisDailyCostIdle" -> 4 * RPisDailyCostIdle,
            "4RPisDailyCostComputing" -> 4 * RPisDailyCostComputing,
            "MinRentingDaysIdleRPiPower" -> MinRentingDaysIdleRPiPower,
            "MinRentingDaysComputingRPiPower" -> MinRentingDaysComputingRPiPower 
          ),
          "E.3" -> ujson.Obj(
            "NbRPisEqBuyingICCM7" -> NbRPisEqBuyingICCM7,
            "RatioRAMRPisVsICCM7" -> RatioRAMRPisVsICCM7,
            "RatioComputeRPisVsICCM7" -> RatioComputeRPisVsICCM7
          )
        )

        val json = write(answers, 4)
        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}

}
