package owl.spark.rdd

import owl.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

object OWLXMLSyntaxOWLAxiomsRDDGenerator extends App {

  val syntax = Syntax.OWLXML
  val input: String = getClass.getResource("/univ-bench.owl").getPath

  /**
  Create a SparkSession, do so by first creating a SparkConf object to configure the application .
  'Local' is a special value that runs Spark on one thread on the local machine, without connecting to a cluster.
  An application name used to identify the application on the cluster manager’s UI.
    */
  val sparkSession = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName("OWL/XML Parser")
    .getOrCreate()

  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger(this.getClass).setLevel(Level.ERROR)
  val RDFXMLBuilder = new OWLXMLSyntaxOWLExpressionsRDDBuilder
  val rdd = RDFXMLBuilder.build(sparkSession, input)

  sparkSession.stop

}
