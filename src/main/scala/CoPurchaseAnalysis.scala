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
    println(s"Using $numPartitions partitions")



    // === TIMER UTILI ===
    val timeStart = System.nanoTime()
    val readStart = System.nanoTime()

    val raw = spark.read
      .csv(inputPath)
      .rdd
      .flatMap(row =>
        Try((row.getString(0).toInt, row.getString(1).toInt)).toOption
      )

    // Partiziono i dati per migliorare l'aggregateByKey
    val rawPartitioned = raw.partitionBy(new HashPartitioner(numPartitions))

    val readEnd = System.nanoTime()
    val groupStart = System.nanoTime()

    val grouped = rawPartitioned
      .aggregateByKey(Set.empty[Int])(
        (set, prod) => set + prod,
        (set1, set2) => set1 ++ set2
      )
    val groupEnd = System.nanoTime()

    val pairStart = System.nanoTime()
    val productPairs = grouped.flatMap {
      case (_, products) =>
        val sorted = products.toList.sorted
        sorted.combinations(2).map {
          case List(p1, p2) => ((p1, p2), 1)
        }
    }

    val pairEnd = System.nanoTime()
    val reduceStart = System.nanoTime()

    val partitionedPairs = productPairs.partitionBy(new HashPartitioner(numPartitions))
    val coPurchaseCounts = partitionedPairs
      .reduceByKey(_ + _)
      .map { case ((p1, p2), count) => s"$p1,$p2,$count" }

    val reduceEnd = System.nanoTime()
    val saveStart = System.nanoTime()

    coPurchaseCounts.saveAsTextFile(outputPath)

    val saveEnd = System.nanoTime()
    val timeEnd = System.nanoTime()

    // === TEMPI IN SECONDI ===
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val timestamp = LocalDateTime.now().format(formatter)

    val readTime   = readEnd - readStart
    val groupTime  = groupEnd - groupStart
    val pairTime   = pairEnd - pairStart
    val reduceTime = reduceEnd - reduceStart
    val saveTime   = saveEnd - saveStart
    val totalTime  = timeEnd - timeStart

    val csvRow = s"$timestamp,$readTime,$groupTime,$pairTime,$reduceTime,$saveTime,$totalTime,$numPartitions\n"

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
        "timestamp,read,group,pairs,reduce,save,total,cores\n" + csvRow
      }

    // Sovrascrive il file (GCS non supporta append nativo)
    val outputStream = fs.create(path, true) // true = overwrite
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    writer.write(updatedContent)
    writer.close()

    spark.stop()
  }
}