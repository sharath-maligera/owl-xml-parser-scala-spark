package owl.spark.rdd

import org.apache.spark.sql.SparkSession
import owl.spark.parsing.{OWLXMLSyntaxExpressionBuilder, OWLXMLSyntaxParsing}

object OWLXMLSyntaxOWLExpressionsRDDBuilder extends Serializable with OWLXMLSyntaxParsing  {

  def build(spark: SparkSession, filePath: String):(OWLExpressionsRDD,OWLExpressionsRDD,OWLExpressionsRDD) = {

    val builder = new OWLXMLSyntaxExpressionBuilder(spark,filePath)
    val (xmlVersionRDD,owlPrefixRDD,owlExpressionsRDD) = builder.getOwlExpressions()
    (xmlVersionRDD,owlPrefixRDD,owlExpressionsRDD)
  }


}
