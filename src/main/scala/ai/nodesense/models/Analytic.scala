package ai.nodesense.models


case class Analytic(id: String, name: String)
case class AnalyticDefinition(id: String, name: String)

case class AnalyticContext(analytic: Analytic, definition: AnalyticDefinition)
