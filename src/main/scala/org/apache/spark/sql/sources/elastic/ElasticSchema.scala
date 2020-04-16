package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.catalyst.expressions._
import org.rzlabs.elastic.ElasticQueryBuilder
import org.rzlabs.elasticsearch.ElasticAttribute

class ElasticSchema(val eqb: ElasticQueryBuilder) {

  def avgExpressions: Map[Expression, (String, String)] = eqb.avgExpressions

  lazy val elasticAttributes: List[ElasticAttribute] = elasticAttrMap.values.toList

  lazy val elasticAttrMap: Map[String, ElasticAttribute] = buildElasticAttr

  lazy val schema: List[Attribute] = elasticAttributes.map {
    case ElasticAttribute(exprId, name, elasticDT, _) =>
      AttributeReference(name, elasticDT)(exprId)
  }

  lazy val pushedDownExprToElasticAttr: Map[Expression, ElasticAttribute] =
    buildPushDownELasticAttrMap

  private def buildPushDownELasticAttrMap: Map[Expression, ElasticAttribute] = {
    eqb.outputAttributeMap.map {
      case (name, (expr, _, _, _)) => expr -> elasticAttrMap(name)
    }
  }

  private def buildElasticAttr: Map[String, ElasticAttribute] = {

    eqb.outputAttributeMap.map {
      case (name, (expr, _, elasticDT, tf)) => {
        val elasticExprId = expr match {
          case null => NamedExpression.newExprId
          case ne: NamedExpression => ne.exprId
          case _ => NamedExpression.newExprId
        }
        (name -> ElasticAttribute(elasticExprId, name, elasticDT, tf))
      }
    }
  }
}
