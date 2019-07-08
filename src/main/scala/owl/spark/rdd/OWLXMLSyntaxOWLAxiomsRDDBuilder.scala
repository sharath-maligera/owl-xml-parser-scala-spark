package owl.spark.rdd

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.io.OWLParserException
import owl.spark.parsing.OWLXMLSyntaxParsing


object OWLXMLSyntaxOWLAxiomsRDDBuilder extends OWLXMLSyntaxParsing {

  private val logger = Logger(this.getClass)

  def build(spark: SparkSession, filePath: String): OWLAxiomsRDD = {
    build(spark, OWLXMLSyntaxOWLExpressionsRDDBuilder.build(spark, filePath))
  }

  def build(spark: SparkSession, owlRecordsRDD: (OWLExpressionsRDD,OWLExpressionsRDD,OWLExpressionsRDD)): OWLAxiomsRDD = {
    val xmlVersionRDD = owlRecordsRDD._1.first()
    val owlPrefixRDD = owlRecordsRDD._2.first()
    val owlExpressionsRDD = owlRecordsRDD._3

    owlExpressionsRDD.map(expressionRDD => {
      try makeAxiom(xmlVersionRDD,owlPrefixRDD,expressionRDD)
      catch {
        case exception: OWLParserException =>
          logger.warn("Parser error for line " + expressionRDD + ": " + exception.getMessage)
          null
      }
    }).filter(_ != null)
  }

}
