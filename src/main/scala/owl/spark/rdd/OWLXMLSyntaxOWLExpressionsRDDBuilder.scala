package owl.spark.rdd

import collection.JavaConverters._
import org.apache.log4j.{Level, Logger}
import org.semanticweb.owlapi.model.OWLAxiom
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import owl.spark.parsing.{ OWLXMLSyntaxParsing, OWLXMLSyntaxPrefixParsing}


class OWLXMLSyntaxOWLExpressionsRDDBuilder extends Serializable with OWLXMLSyntaxParsing with OWLXMLSyntaxPrefixParsing {

  def build(spark: SparkSession, filePath: String): OWLAxiomsRDD = {

    val owlClassConfig = new JobConf()
    val owlDatatypePropertyConfig = new JobConf()
    val owlObjectPropertyConfig = new JobConf()
    val owlPrefixes = new JobConf()

    owlClassConfig.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    owlClassConfig.set("stream.recordreader.begin", "<owl:") // start Tag
    owlClassConfig.set("stream.recordreader.end", "</owl:Class>") // End Tag

    owlObjectPropertyConfig.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    owlObjectPropertyConfig.set("stream.recordreader.begin", "<owl:ObjectProperty") // start Tag
    owlObjectPropertyConfig.set("stream.recordreader.end", "</owl:ObjectProperty>") // End Tag

    owlDatatypePropertyConfig.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    owlDatatypePropertyConfig.set("stream.recordreader.begin", "<owl:DatatypeProperty") // start Tag
    owlDatatypePropertyConfig.set("stream.recordreader.end", "</owl:DatatypeProperty>") // End Tag

    owlPrefixes.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    owlPrefixes.set("stream.recordreader.begin", "<rdf:RDF") // start Tag
    owlPrefixes.set("stream.recordreader.end", ">") // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(owlClassConfig, filePath)
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(owlDatatypePropertyConfig, filePath)
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(owlObjectPropertyConfig, filePath)
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(owlPrefixes, filePath)

    // RDF XML Record
    // read data and save in RDD as block- RDFXML Record
    val owlClassRecord: RDD[(Text, Text)] = spark.sparkContext.hadoopRDD(owlClassConfig,
      classOf[org.apache.hadoop.streaming.StreamInputFormat], //input format
      classOf[org.apache.hadoop.io.Text], //class for the key
      classOf[org.apache.hadoop.io.Text]) //class for the value

    val OWLXML_Prefixes = spark.sparkContext.hadoopRDD(owlPrefixes,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])

    val owlDatatypePropertyRecord = spark.sparkContext.hadoopRDD(owlDatatypePropertyConfig,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])

    val owlObjectPropertyRecord = spark.sparkContext.hadoopRDD(owlObjectPropertyConfig,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])

    // Convert the block- RDFXML Record to String DataType
    val rawOwlClassRDD: RDD[String] = owlClassRecord.map { case (x, y) => (x.toString()) }
    val rawOwlDatatypePropertyRecord: RDD[String] = owlDatatypePropertyRecord.map { case (x, y) => (x.toString()) }
    val rawOwlObjectPropertyRecord: RDD[String] = owlObjectPropertyRecord.map { case (x, y) => (x.toString()) }
    var tmp_Prefixes = OWLXML_Prefixes.map { case (x, y) => (x.toString()) }.distinct()


    val pref: String = tmp_Prefixes.reduce((a, b) => a + "\n" + b)

    val OWLClassAxiomsList = rawOwlClassRDD.map(record => RecordParse(record, pref))
      .filter(x => !x.isEmpty)

    val OWLDatatypeAxiomsList = rawOwlDatatypePropertyRecord.map(record => RecordParse(record, pref))
      .filter(x => !x.isEmpty)

    val OWLObjectPropertyAxiomsList = rawOwlObjectPropertyRecord.map(record => RecordParse(record, pref))
      .filter(x => !x.isEmpty)

    var OWLAxiomsList = OWLClassAxiomsList ++ OWLDatatypeAxiomsList ++ OWLObjectPropertyAxiomsList
    val OWLAxiomsRDD: RDD[OWLAxiom] = OWLAxiomsList.flatMap(line => line.iterator().asScala)
      .distinct()

    OWLAxiomsRDD.foreach(println(_))

    println("Axioms count = " + OWLAxiomsRDD.count())

    OWLAxiomsRDD
  }

}

object OWLXMLSyntaxOWLExpressionsRDDBuilder {

  def main(args: Array[String]): Unit = {

    val input: String = getClass.getResource("/univ-bench.owl").getPath

    println("================================")
    println("|        OWL/XML Parser        |")
    println("================================")



    @transient val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("OWL/XML Parser")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger(this.getClass).setLevel(Level.ERROR)
    // Logger.getRootLogger().setLevel(Level.OFF)




    val RDFXMLBuilder = new OWLXMLSyntaxOWLExpressionsRDDBuilder
    val rdd = RDFXMLBuilder.build(sparkSession, input)

    sparkSession.stop
  }
}
