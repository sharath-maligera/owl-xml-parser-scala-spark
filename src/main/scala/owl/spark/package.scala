package owl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import owl.spark.rdd._

package object spark {

  object Syntax extends Enumeration {
    val OWLXML = Value
  }

  /**
    * Implicit class to wrap Syntax Value and provide owlxml method to build a [[SparkSession]] that allows to read owl files.
    */
  /*implicit class OWLAxiomReader(spark: SparkSession) {

    /**
      * Load OWL data into a RDD[OWLAxiom] of OWL/XML syntax
      * @param syntax: OWL syntax of type String
      * @return RDD[OWLAxiom]s: returns RDD[OWLAxioms]
      */
    def owl(syntax: Syntax.Value): String => OWLAxiomsRDD = syntax match {
      case Syntax.OWLXML => owlxml
      case _ => throw new IllegalArgumentException(s"${syntax} syntax not integrated!")
    }

    /**
      * Load OWL data in OWL/XML syntax into an RDD[OWLAxiom].
      * @return the RDD[OWLAxiomsRDD]
      */
    def owlxml: String => OWLAxiomsRDD = path => {
      OWLXMLSyntaxOWLAxiomsRDDBuilder.build(spark, path)
    }

  }*/


  /**
    * Implicit class to wrap Syntax Value and provide owlxml method to build [[SparkSession]] that allows to read owl files.
    */
  /*implicit class OWLExpressionsRDDReader(spark: SparkSession) {

    /**
      * Load OWL expressions(character snippets extracted in OWL XML syntax i.e "<owl: to </owl:>") into a RDD[String].
      * @param syntax: OWL syntax of type String
      * @return a RDD[OWLExpressionsRDD]
      */
    def owlExpressions(syntax: Syntax.Value): String => OWLExpressionsRDD = syntax match {
      case Syntax.OWLXML => owlxml
      case _ => throw new IllegalArgumentException(s"${syntax} syntax not integrated!")
    }

    /**
      * Load OWL data in Functional syntax into an [[RDD]][String].
      * @return the [[OWLExpressionsRDD]]
      */
    def owlxml: String => OWLExpressionsRDD = path => {
      System.out.println(path)
      // OWLXMLSyntaxOWLExpressionsRDDBuilder.build(spark, path)
    }

  }*/

}
