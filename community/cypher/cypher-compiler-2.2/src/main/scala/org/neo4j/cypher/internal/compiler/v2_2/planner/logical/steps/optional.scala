/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.QueryPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.CandidateList
import org.neo4j.cypher.internal.compiler.v2_2.ast.PatternExpression
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph

/**
 * Only used for the specific case where we don't have any arguments, e.g. MATCH n OPTIONAL MATCH m-[:X]->(x) RETURN n, x
 */
object optional extends CandidateGenerator[PlanTable] {
  def apply(ignored: PlanTable, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): CandidateList = {
    val optionalCandidates =
      for (optionalQG <- queryGraph.optionalMatches if optionalQG.argumentIds.isEmpty)
      yield {
        val rhs = context.strategy.plan(optionalQG)

        planOptional(rhs)
      }

    CandidateList(optionalCandidates)
  }
}
