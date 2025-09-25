import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.HashPartitioner
import scala.util.{Try, Success, Failure}
import java.net.URI

object CoPurchaseAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Co-Purchase Analysis")
      .getOrCreate()

    val sc = spark.sparkContext

    // === Percorsi ===
    val workers = args(0)         //Questa info deve essere uguale al parametro di creazione del cluster
    val coresPerWorker = args(1)  //Questa info deve essere uguale al parametro di creazione del cluster
    val inputPath = args(2)
    val outputPath = args(3)      //ATTENZIONE: cancellare i precedenti risultati prima di far partire il job
    val logPath = args(4)         //I log vengono scritti in append

    val numPartitions = workers.toInt * coresPerWorker.toInt * 3
    println(s"CO-PUR: Using $numPartitions partitions")

    // === TIMER UTILI ===
    val timeStart = System.nanoTime()
    val raw = spark.read
      .csv(inputPath)
      .rdd
      .flatMap(row =>
        Try((row.getString(0).toInt, row.getString(1).toInt)).toOption
      )

    // Partiziono i dati per migliorare l'aggregateByKey
    val rawPartitioned = raw.partitionBy(new HashPartitioner(numPartitions))


    val grouped = rawPartitioned
      .aggregateByKey(Set.empty[Int])(
        (set, prod) => set + prod,
        (set1, set2) => set1 ++ set2
      )

    val productPairs = grouped.flatMap {
      case (_, products) =>
        val sorted = products.toList.sorted
        sorted.combinations(2).map {
          case List(p1, p2) => ((p1, p2), 1)
        }
    }

    val partitionedPairs = productPairs.partitionBy(new HashPartitioner(numPartitions))
    val coPurchaseCounts = partitionedPairs
      .reduceByKey(_ + _)
      .map { case ((p1, p2), count) => s"$p1,$p2,$count" }

    coPurchaseCounts.repartition(1).saveAsTextFile(outputPath)

    println(s"CO-PUR: Wrote output file on $outputPath")

    val timeEnd = System.nanoTime()

    // === TEMPI IN SECONDI ===
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val timestamp = LocalDateTime.now().format(formatter)

    val totalTime  = timeEnd - timeStart

    val csvRow = s"$timestamp,$totalTime,$numPartitions\n"

    // === APPEND TO CSV ===
    val conf = sc.hadoopConfiguration
    val uri = new URI(logPath) // logPath = "gs://..."
    val fs = FileSystem.get(uri, conf)
    val path = new Path(logPath)

    val updatedContent =
      if (fs.exists(path)) {
        // Legge tutto il contenuto esistente
        val existingStream = fs.open(path)
        val reader = new BufferedReader(new InputStreamReader(existingStream))
        val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
        reader.close()
        lines + "\n" + csvRow
      } else {
        // Intestazione + prima riga
        "timestamp,cores\n" + csvRow
      }

    // Sovrascrive il file (GCS non supporta append nativo)
    val outputStream = fs.create(path, true) // true = overwrite
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    writer.write(updatedContent)
    writer.close()
    println(s"CO-PUR: Wrote log file on $logPath")

    spark.stop()
  }
}