package owl.spark.parsing

import java.io.{ByteArrayInputStream,IOException, PipedInputStream, PipedOutputStream}
import java.util.ArrayList
//import java.util.stream.Stream

import scala.collection.immutable.Stream
import scala.compat.java8.StreamConverters._
import scala.util.matching.Regex
import collection.JavaConverters._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.apache.jena.rdf.model.{Model, ModelFactory, Statement}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.OWLXMLDocumentFormat
import org.semanticweb.owlapi.functional.parser.OWLFunctionalSyntaxOWLParserFactory
import org.semanticweb.owlapi.io.{OWLParserException, StringDocumentSource}
import org.semanticweb.owlapi.model.{AxiomType, OWLAnnotationPropertyDomainAxiom, OWLAxiom, OWLDeclarationAxiom, OWLOntology, OWLSubAnnotationPropertyOfAxiom}
import org.semanticweb.owlapi.util.OWLAPIStreamUtils.asList
import org.slf4j.{Logger, LoggerFactory}
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.rdf.rdfxml.renderer.OWLOntologyXMLNamespaceManager
import org.semanticweb.owlapi.search.EntitySearcher

import scala.collection.immutable.Stream
import scala.compat.java8.StreamConverters._

object OWLXMLSyntaxParsing {

  /** marker used to store the prefix for the default namespace */
  val _empty = "_EMPTY_"
  // TODO: refine
  val prefixPattern = "Prefix\\(([a-zA-Z]*)\\:=<(.*)>\\)".r
  val ontologyPattern = "Ontology\\(<(.*)>".r
}


/**
  * Trait to support the parsing of input OWL files in functional syntax. This
  * trait mainly defines how to make axioms from input functional syntax
  * expressions.
  */
trait OWLXMLSyntaxParsing {
  private def man = OWLManager.createOWLOntologyManager()

  private def dataFactory = man.getOWLDataFactory

  val LOGGER: Logger = LoggerFactory.getLogger(classOf[OWLXMLSyntaxExpressionBuilder])
  // private var obj = OWLObject

  // private def ontConf = man.getOntologyLoaderConfiguration


  def RecordParse(record: String, prefixes: String): ArrayList[OWLAxiom] = {

    var OWLAxioms = new ArrayList[OWLAxiom]()

    // val model = ModelFactory.createDefaultModel()
    val modelText = "<?xml version=\"1.0\" encoding=\"utf-8\" ?> \n" + prefixes + record + "</rdf:RDF>"
    // model.read(new ByteArrayInputStream(modelText.getBytes()), "http://swat.cse.lehigh.edu")

    /*val iter = model.listStatements()

    while (iter.hasNext()) {
      val stmt = iter.next()
      val axiomSet = makeAxiom(stmt).toList.asJavaCollection
      // System.out.println("AxiomSet" + axiomSet)
      OWLAxioms.addAll(axiomSet)
    }*/


    val manager = OWLManager.createOWLOntologyManager
    val ontology = try {
      manager.loadOntologyFromOntologyDocument(new StringDocumentSource(modelText))
    } catch {
      case e: OWLOntologyCreationException => null
    }

    if (ontology!=null){
      val format = manager.getOntologyFormat(ontology)

      val owlxmlFormat = new OWLXMLDocumentFormat
      if (format != null && format.isPrefixOWLDocumentFormat) { // copy all the prefixes from OWL document format to OWL XML format
        owlxmlFormat.copyPrefixesFrom(format.asPrefixOWLDocumentFormat)
      }

      try{
        manager.saveOntology(ontology)
      } catch {
        case e: OWLOntologyStorageException => null
      }

      val factory = manager.getOWLDataFactory
      var axioms = ontology.axioms()
      // System.out.println("\nLoaded ontology with " + ontology.getAxiomCount() + " axioms")
      val axiomResult= ontology.axioms().toScala[Stream].toSet.toList.asJavaCollection
      OWLAxioms.addAll(axiomResult)
    }
    OWLAxioms
  }

  /**
    * Builds a snippet conforming to the RDFXML syntax which then can
    * be parsed by the OWLAPI RDFXML syntax parser.
    * A single expression, e.g.
    *
    * <rdf:RDF
    * xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    * xmlns:owl="http://www.w3.org/2002/07/owl#">
    * <owl:Class rdf:about="http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"/>
    * </rdf:RDF>
    *
    * then we will get
    * Declaration(Class("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"))
    *
    * @param st A Statement containing an expression in RDFXML syntax
    * @return The set of axioms corresponding to the expression.
    */

  def makeAxiom(st: Statement): Set[OWLAxiom] = {

    val model = ModelFactory.createDefaultModel

    model.add(st)

    val ontology: OWLOntology = getOWLOntology(model)

    val axiomSet: Set[OWLAxiom] = ontology.axioms().toScala[Stream].toSet

    axiomSet
  }

  /**
    * To get the corresponding OWLOntology of a certain model
    *
    * @param model Model
    * @return OWLOntology corresponding to that model
    */

  def getOWLOntology(model: Model): OWLOntology = {
    val in = new PipedInputStream
    val o = new PipedOutputStream(in)

    new Thread(new Runnable() {
      def run(): Unit = {
        model.write(o, "RDF/XML-ABBREV")
        //     model.write(System.out, "RDF/XML")

        try {
          o.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }

      }
    }).start()
    val ontology: OWLOntology = man.loadOntologyFromOntologyDocument(in)
    ontology

  }

  def refineOWLAxioms(sc: SparkContext, axiomsRDD: RDD[OWLAxiom]): RDD[OWLAxiom] = {

    axiomsRDD.cache()

    /**
      * Case 1: Handler for wrong domain axioms
      * OWLDataPropertyDomain and OWLObjectPropertyDomain
      * converted wrong to OWLAnnotationPropertyDomain
      */

    val declaration = extractAxiom(axiomsRDD, AxiomType.DECLARATION)
      .asInstanceOf[RDD[OWLDeclarationAxiom]]
    declaration.foreach(System.out.println(_))

    val dataProperties = declaration.filter(a => a.getEntity.isOWLDataProperty)
      .map(a => a.getEntity.getIRI)
    dataProperties.foreach(System.out.println(_))

    val objectProperties = declaration.filter(a => a.getEntity.isOWLObjectProperty)
      .map(a => a.getEntity.getIRI)
    objectProperties.foreach(System.out.println(_))

    val dd = dataProperties.collect()
    dd.foreach(System.out.println(_))
    val oo = objectProperties.collect()
    oo.foreach(System.out.println(_))

    val dataBC = sc.broadcast(dd)
    val objBC = sc.broadcast(oo)

    val annDomain = extractAxiom(axiomsRDD, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
    annDomain.foreach(System.out.println(_))

    val domainTransform = annDomain.asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .map { a =>

        val prop = a.getProperty.getIRI
        val domain = a.getDomain

        // Annotation property domain axiom turned to object property domain after parsing.
        if (objBC.value.contains(prop)) {

          val c = dataFactory.getOWLClass(domain)
          val p = dataFactory.getOWLObjectProperty(prop.toString)
          val obj = dataFactory.getOWLObjectPropertyDomainAxiom(p, c, asList(a.annotations))

          obj

        } else if (dataBC.value.contains(prop)) {

          // Annotation property domain axiom turned to object property domain after parsing.
          val c = dataFactory.getOWLClass(domain)
          val p = dataFactory.getOWLDataProperty(prop.toString)
          val obj = dataFactory.getOWLDataPropertyDomainAxiom(p, c, asList(a.annotations))

          obj

        } else {

          val obj = dataFactory
            .getOWLAnnotationPropertyDomainAxiom(a.getProperty.asOWLAnnotationProperty, domain, asList(a.annotations))
            .asInstanceOf[OWLAxiom]
          obj

        }
      }
    domainTransform.foreach(System.out.println(_))

    /**
      * Case 2: Handler for wrong subPropertyOf axioms
      * OWLSubPropertyOf converted wrong to OWLSubAnnotationPropertyOf
      * instead of OWLSubDataPropertyOf or OWLSubObjectPropertyOf
      */

    val subAnnotationProperty = extractAxiom(axiomsRDD, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
    subAnnotationProperty.foreach(System.out.println(_))

    val subPropertyTransform = subAnnotationProperty.asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .map { a =>

        val subProperty = a.getSubProperty.getIRI
        val supProperty = a.getSuperProperty.getIRI


        // SubAnnotationPropertyOf axiom turned to SubObjectPropertyOf after parsing.
        if (objBC.value.contains(subProperty)) {

          val sub = dataFactory.getOWLObjectProperty(subProperty.toString)
          val sup = dataFactory.getOWLObjectProperty(supProperty.toString)
          val obj = dataFactory.getOWLSubObjectPropertyOfAxiom(sub, sup, asList(a.annotations))

          obj

        } else if (dataBC.value.contains(subProperty)) {

          // SubAnnotationPropertyOf axiom turned to SubDataPropertyOf after parsing.
          val sub = dataFactory.getOWLDataProperty(subProperty.toString)
          val sup = dataFactory.getOWLDataProperty(supProperty.toString)
          val obj = dataFactory.getOWLSubDataPropertyOfAxiom(sub, sup, asList(a.annotations))

          obj

        } else {

          val obj = dataFactory
            .getOWLSubAnnotationPropertyOfAxiom(a.getSubProperty.asOWLAnnotationProperty,
              a.getSuperProperty.asOWLAnnotationProperty(),
              asList(a.annotations))
            .asInstanceOf[OWLAxiom]
          obj

        }
      }
    subPropertyTransform.foreach(System.out.println(_))

    val differenceRDD = axiomsRDD.subtract(annDomain).subtract(subAnnotationProperty)
    differenceRDD.foreach(System.out.println(_))
    val correctRDD = sc.union(differenceRDD, domainTransform, subPropertyTransform)

    correctRDD.foreach(println(_))

    correctRDD
  }

  def extractAxiom(axiom: RDD[OWLAxiom], T: AxiomType[_]): RDD[OWLAxiom] = {
    axiom.filter(a => a.getAxiomType.equals(T))
  }
}


/**
  * Trait to support the parsing of prefixes from expressions given in
  * functional syntax.
  */
trait OWLXMLSyntaxPrefixParsing {
  /**
    * Parses the prefix declaration of a namespace URI and returns the
    * pair (prefix, namespace URI)
    *
    * @param prefixLine Sth like
    *                   Prefix(:=<http://purl.obolibrary.org/obo/pato.owl#>) or
    *                   Prefix(dc:=<http://purl.org/dc/elements/1.1/>)
    */
  def parsePrefix(prefixLine: String): (String, String) = {
    var prefix, uri: String = null

    prefixLine.trim match {
      case OWLXMLSyntaxParsing.prefixPattern(p, u) =>
        prefix = p
        uri = u
    }

    if (prefix.isEmpty) prefix = OWLXMLSyntaxParsing._empty

    (prefix, uri)
  }

  def isPrefixDeclaration(expression: String): Boolean = {
    OWLXMLSyntaxParsing.prefixPattern.pattern.matcher(expression).matches()
  }
}


/**
  * The main purpose of this class is to provide an object which is initialized
  * with
  *
  * - all prefix declarations
  *
  * and, fed with an expression in functional syntax, returns expressions in
  * functional syntax, with
  *
  * - all prefix URIs being extended
  * - all comments removed
  * - all non-axiom expressions (e.g. Ontology(...), Prefix(...)) removed
  *
  * 'Removed' here means that a null value is returned discarding the input
  * string.
  *
  * @param prefixes Map containing all read prefixes; required to expand
  *                 namespace shortcuts in the cleaned (functional syntax)
  *                 axiom string
  */
class OWLXMLSyntaxExpressionBuilder(val prefixes: Map[String, String]) extends Serializable {
  def clean(expression: String): String = {
    var trimmedExpr = expression.trim

    /* Throw away expressions that are of no use for further processing:
     * 1) empty lines
     * 2) comments
     * 3) the last, outermost closing parenthesis
     * 4) prefix declaration
     * 5) ontology declaration
     * 6) URL being part of the ontology declaration, e.g.
     *    <http://purl.obolibrary.org/obo/pato/releases/2016-05-22/pato.owl>
     *    being part of Ontology(...)
     */
    val discardExpression: Boolean =
      trimmedExpr.isEmpty  || // 1)
        trimmedExpr.startsWith("#") ||  // 2)
        trimmedExpr.startsWith(")") ||  // 3)
        trimmedExpr.startsWith("Prefix(") ||  // 4)
        trimmedExpr.startsWith("Ontology(") ||  // 5)
        trimmedExpr.startsWith("<http") // 6

    if (discardExpression) {
      null

    } else {
      // Expand prefix abbreviations: foo:Bar --> http://foo.com/somePath#Bar
      for (prefix <- prefixes.keys) {
        val p = prefix + ":"

        if (trimmedExpr.contains(p)) {
          val v: String = "<" + prefixes.get(prefix).get
          // TODO: refine regex
          val pattern = (p + "([a-zA-Z][0-9a-zA-Z_-]*)\\b").r

          // Append ">" to all matched local parts: "foo:car" --> "foo:car>"
          trimmedExpr = pattern.replaceAllIn(trimmedExpr, m => s"${m.matched}>")
          // Expand prefix: "foo:car>" --> "http://foo.org/res#car>"
          trimmedExpr = trimmedExpr.replace(p.toCharArray, v.toCharArray)
        }
      }

      // handle default prefix e.g. :Bar --> http://foo.com/defaultPath#Bar
      // TODO: refine regex
      val pattern = ":[^/][a-zA-Z][0-9a-zA-Z_-]*".r
      val v: String = "<" + prefixes.get(OWLXMLSyntaxParsing._empty).get

      if (prefixes.contains(OWLXMLSyntaxParsing._empty)) {
        pattern.findAllIn(trimmedExpr).foreach(hit => {
          val full = hit.replace(":".toCharArray, v.toCharArray)
          trimmedExpr = trimmedExpr.replace(hit, full + ">")
        })
      }

      trimmedExpr
    }
  }
}
