package owl.spark.rdd

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.io.OWLParserException
import owl.spark.parsing.OWLXMLSyntaxParsing

object OWLXMLSyntaxOWLAxiomsRDDBuilder extends OWLXMLSyntaxParsing {

  private val logger = Logger(this.getClass)

  /*def build(spark: SparkSession, filePath: String): OWLAxiomsRDD = {
    build(spark, OWLXMLSyntaxOWLExpressionsRDDBuilder.build(spark, filePath))
  }

  // FIXME: It has to be ensured that expressionsRDD is in functional syntax
  def build(spark: SparkSession, expressionsRDD: OWLExpressionsRDD): OWLAxiomsRDD = {
    expressionsRDD.map(expression => {
      try makeAxiom(expression)
      catch {
        case exception: OWLParserException =>
          logger.warn("Parser error for line " + expression + ": " + exception.getMessage)
          null
      }
    }).filter(_ != null)
  }*/

}
