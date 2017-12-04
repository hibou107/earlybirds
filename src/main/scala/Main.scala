import java.io.{File, PrintWriter}

import scala.util.Try


case class DataState(
                      currentUserIndex: Int,
                      userMap: Map[String, Int],
                      currentProductIndex: Int,
                      productMap: Map[String, Int],
                      maxTimestamp: Long
                    )

//  userId,itemId,rating,timestamp
case class RawData(userId: String, itemId: String, rating: Float, timestamp: Long)


object Const {
  val PENALTY: Float = 0.95F
  val LOWEST_SCORE: Float = 0.01F
}
object Main extends App {


  private def processLine(line: String): Option[RawData] = {
    val cols = line.split(",").map(_.trim)
    if (cols.length != 4)
      None
    else {
      val userId = cols(0)
      val itemId = cols(1)
      val ratingOpt = Try(cols(2).toFloat).toOption match {
        case None =>
          println(s"Cannot parse rating ${cols(2)}")
          None
        case Some(value) => Some(value)
      }
      val timestampOpt = Try(cols(3).toLong).toOption match {
        case None =>
          println(s"Cannot parse timestamp ${cols(3)}")
          None
        case Some(value) => Some(value)
      }
      for {
        rating <- ratingOpt
        timestamp <- timestampOpt
      } yield RawData(userId, itemId, rating, timestamp)

    }
  }

  private def updateState(currentState: DataState, rawDataOpt: Option[RawData]): DataState = {
    rawDataOpt match {
      case None => currentState
      case Some(rawData) =>
        val updatedMaxTimestamp =
          if (rawData.timestamp > currentState.maxTimestamp)
            rawData.timestamp
          else
            currentState.maxTimestamp

        val (updatedUserIndex, updatedUserMap) = currentState.userMap.get(rawData.userId) match {
          case None =>
            val nextIndex = currentState.currentUserIndex + 1
            (nextIndex, currentState.userMap + (rawData.userId -> currentState.currentUserIndex))
          case Some(_) => (currentState.currentUserIndex, currentState.userMap)
        }

        val (updateProductIndex, updateProductMap) = currentState.productMap.get(rawData.itemId) match {
          case None =>
            val nextIndex = currentState.currentProductIndex + 1
            (nextIndex, currentState.productMap + (rawData.itemId -> currentState.currentProductIndex))
          case Some(_) => (currentState.currentProductIndex, currentState.productMap)
        }
        DataState(updatedUserIndex, updatedUserMap, updateProductIndex, updateProductMap, updatedMaxTimestamp)

    }
  }


  private def writeToFile(file: String, text: Iterator[String]): Unit = {
    val pw = new PrintWriter(new File(file))
    text.foreach(row =>
      pw.write(s"$row\n")
    )
    pw.close()
  }


  def compute(penalty: Double, lowerScore: Double, input: () => Iterator[String]): (DataState, Map[(Int, Int), Float]) = {
    val initState = DataState(0, Map.empty, 0, Map.empty, 0)

    val preComputed = input().foldLeft(initState) { case (current, line) =>
      updateState(current, processLine(line))
    }

    val initResult = Map.empty[(Int, Int), Float]

    val maxDate = preComputed.maxTimestamp


    val result = input().foldLeft(initResult) { case (currentState, line) =>
      val rawDataOpt = processLine(line)

      if (rawDataOpt.isEmpty) {
        println(s"Cannot process line $line")
      }

      // If rawData can not be parsed, we just show a warning message and continue

      rawDataOpt.flatMap { rawData =>
        val userIdOpt = preComputed.userMap.get(rawData.userId)
        val productIdOpt = preComputed.productMap.get(rawData.itemId)
        (userIdOpt, productIdOpt) match {
          case (Some(userId), Some(productId)) =>
            val currentValue = currentState.getOrElse((userId, productId), 0.0F)
            val currentDate = rawData.timestamp
            val diffDate = ((maxDate - currentDate) / 86400000).toInt // 3600 * 1000 * 24
            if (diffDate < 0)
              throw new RuntimeException(s"Diff date $diffDate ($maxDate - $currentDate) should never be negative, the program failed!")

            val plusValue = Math.pow(penalty, diffDate).toFloat
            Some(currentState + ((userId, productId) -> (currentValue + rawData.rating * plusValue)))

          case _ =>
            throw new RuntimeException("UserId or productId cannot be found in Map")
        }
      }.getOrElse(currentState)
    }

    // filtering before return the values
    val filteredResult = result.filter(_._2 > lowerScore)

    (preComputed, filteredResult)

  }

  override def main(args: Array[String]): Unit = {
    println("Enter the file path")
    val csvPath = scala.io.StdIn.readLine()
    val (computed, result) = compute(Const.PENALTY, Const.LOWEST_SCORE, () => io.Source.fromFile(csvPath).getLines())

    // Now output results
    println("Enter output folder:")
    val outputPath = scala.io.StdIn.readLine()

    writeToFile(outputPath + "/lookupuser.csv", computed.userMap.toIterator.map { case (user, userIdInt) =>
        s"$user,$userIdInt"
    })

    writeToFile(outputPath + "/lookup_product.csv", computed.productMap.toIterator.map { case (item, itemIdInt) =>
      s"$item,$itemIdInt"
    })

    writeToFile(outputPath + "/aggratings.csv", result.toIterator.map { case ((userId, itemId), ratingSum) =>
      s"$userId,$itemId,$ratingSum"
    })

  }

}
