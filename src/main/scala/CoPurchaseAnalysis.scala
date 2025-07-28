import org.apache.spark.sql.{SparkSession}
import org.apache.spark.rdd.RDD

object CoPurchaseAnalysis {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: CoPurchaseAnalysis <input_path> <output_path>")
      sys.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder()
      .appName("CoPurchaseAnalysis")
      .master("local[*]") // Usa tutti i core disponibili
      .getOrCreate()

    val sc = spark.sparkContext

    // Carica il CSV: ogni riga è (order_id, product_id)
    val rawData: RDD[(Int, Int)] = sc
      .textFile(inputPath)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { line =>
        val Array(orderId, productId) = line.split(",").map(_.toInt)
        (orderId, productId)
      }

    // Inizio misurazione tempo
    val startTime = System.nanoTime()

    // Gruppo per ordine: Map(orderId -> List(productId))
    val ordersGrouped: RDD[(Int, Iterable[Int])] = rawData.groupByKey() //Dovrebbe causare shuffle

    // Per ogni ordine, genero tutte le coppie uniche (x, y) con x < y
    val productPairs: RDD[((Int, Int), Int)] = ordersGrouped.flatMap {
      case (_, products) =>
        val prodList = products.toSet.toList.sorted
        for {
          i <- prodList.indices
          j <- (i + 1) until prodList.length
        } yield ((prodList(i), prodList(j)), 1)
    }

    // Sommo le occorrenze di ciascuna coppia
    val coPurchaseCounts: RDD[(String)] = productPairs
      .reduceByKey(_ + _)
      .map { case ((p1, p2), count) => s"$p1,$p2,$count" }

    val count = coPurchaseCounts.count()

    // Fine misurazione tempo
    val endTime = System.nanoTime()
    val durationMs = (endTime - startTime) / 1e6

    println(f"Tempo di esecuzione co-purchase: $durationMs%.2f ms")

    // Salvo l’output in CSV
    coPurchaseCounts.coalesce(1).saveAsTextFile(outputPath + ".csv")

    spark.stop()
  }
}