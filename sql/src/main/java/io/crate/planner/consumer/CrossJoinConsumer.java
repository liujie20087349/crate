/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.exceptions.ValidationException;
import io.crate.operation.projectors.TopN;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.fetch.FetchRequiredVisitor;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.*;


@Singleton
public class CrossJoinConsumer implements Consumer {

    private final Visitor visitor;

    @Inject
    public CrossJoinConsumer(ClusterService clusterService,
                             AnalysisMetaData analysisMetaData) {
        visitor = new Visitor(clusterService, analysisMetaData);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ClusterService clusterService;
        private final AnalysisMetaData analysisMetaData;

        public Visitor(ClusterService clusterService, AnalysisMetaData analysisMetaData) {
            this.clusterService = clusterService;
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect statement, ConsumerContext context) {
            if (isUnsupportedStatement(statement, context)) return null;
            if (statement.querySpec().where().noMatch()) {
                return new NoopPlannedAnalyzedRelation(statement, context.plannerContext().jobId());
            }

            final Collection<QualifiedName> relationNames = getOrderedRelationNames(statement);

            // TODO: replace references with docIds.. and add fetch projection



            List<QueriedTableRelation> queriedTables = new ArrayList<>(relationNames.size());
            for (QualifiedName relationName : relationNames) {
                MultiSourceSelect.Source source = statement.sources().get(relationName);
                QueriedTableRelation queriedTable;
                try {
                    queriedTable = SubRelationConverter.INSTANCE.process(source.relation(), source);
                } catch (ValidationException e) {
                    context.validationException(e);
                    return null;
                }
                queriedTables.add(queriedTable);
            }

            WhereClause where = statement.querySpec().where();
            if (where.hasQuery() && !(where.query() instanceof Literal)) {
                throw new UnsupportedOperationException("JOIN condition in the WHERE clause is not supported");
            }

            // for nested loops we are fine to remove pushed down orders
            OrderBy remainingOrderBy = statement.remainingOrderBy();
            statement.querySpec().orderBy(remainingOrderBy);

            // replace all the fields in the root query spec
            RelationSplitter.replaceFields(queriedTables, statement.querySpec());


            //TODO: queriedTable.normalize(analysisMetaData);


            QueriedTableRelation<?> left = queriedTables.get(0);
            QueriedTableRelation<?> right = queriedTables.get(1);

            if (remainingOrderBy != null) {
                for (QueriedTableRelation queriedTable : queriedTables) {
                    queriedTable.querySpec().limit(null);
                    queriedTable.querySpec().offset(TopN.NO_OFFSET);
                }
            }

            Integer limit = statement.querySpec().limit();
            if (limit != null) {
                context.requiredPageSize(limit + statement.querySpec().offset());
            }

            // this normalization is required to replace fields of the table relations
            left.normalize(analysisMetaData);
            right.normalize(analysisMetaData);

            // plan the subRelations
            PlannedAnalyzedRelation leftPlan = context.plannerContext().planSubRelation(left, context);
            PlannedAnalyzedRelation rightPlan = context.plannerContext().planSubRelation(right, context);
            context.requiredPageSize(null);

            Set<String> localExecutionNodes = ImmutableSet.of(clusterService.localNode().id());

            MergePhase leftMerge = mergePhase(
                    context,
                    localExecutionNodes,
                    leftPlan.resultPhase(),
                    left.querySpec());
            MergePhase rightMerge = mergePhase(
                    context,
                    localExecutionNodes,
                    rightPlan.resultPhase(),
                    right.querySpec());

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(analysisMetaData.functions(), statement.querySpec());

            TopNProjection topN = projectionBuilder.topNProjection(
                    concatFields(left, right),
                    statement.querySpec().orderBy(),
                    statement.querySpec().offset(),
                    statement.querySpec().limit(),
                    statement.querySpec().outputs()
            );

            NestedLoopPhase nl = new NestedLoopPhase(
                    context.plannerContext().jobId(),
                    context.plannerContext().nextExecutionPhaseId(),
                    "nested-loop",
                    ImmutableList.<Projection>of(topN),
                    leftMerge,
                    rightMerge,
                    localExecutionNodes
            );
            return new NestedLoop(nl, leftPlan, rightPlan, true);
        }

        private static List<Field> concatFields(QueriedTableRelation<?> left, QueriedTableRelation<?> right) {
            List<Field> inputs = new ArrayList<>(left.fields().size() + right.fields().size());
            inputs.addAll(left.fields());
            inputs.addAll(right.fields());
            return inputs;
        }

        private MergePhase mergePhase(ConsumerContext context,
                                      Set<String> localExecutionNodes,
                                      UpstreamPhase upstreamPhase,
                                      QuerySpec querySpec) {
            assert !upstreamPhase.executionNodes().isEmpty() : "upstreamPhase must be executed somewhere";
            if (upstreamPhase.executionNodes().equals(localExecutionNodes)) {
                // if the nested loop is on the same node we don't need a mergePhase to receive requests
                // but can access the RowReceiver of the nestedLoop directly
                return null;
            }

            OrderBy orderBy = querySpec.orderBy();
            List<Symbol> previousOutputs = querySpec.outputs();

            MergePhase mergePhase;
            if (OrderBy.isSorted(orderBy)) {
                mergePhase = MergePhase.sortedMerge(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        orderBy,
                        previousOutputs,
                        orderBy.orderBySymbols(),
                        ImmutableList.<Projection>of(),
                        upstreamPhase.executionNodes().size(),
                        Symbols.extractTypes(previousOutputs)
                );
            } else {
                mergePhase = MergePhase.localMerge(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        ImmutableList.<Projection>of(),
                        upstreamPhase.executionNodes().size(),
                        Symbols.extractTypes(previousOutputs)
                );
            }
            mergePhase.executionNodes(localExecutionNodes);
            return mergePhase;
        }

        /**
         * returns a map with the relation as keys and the values are their order in occurrence in the order by clause.
         * <p/>
         * e.g. select * from t1, t2, t3 order by t2.x, t3.y
         * <p/>
         * returns: {
         * t2: 0                 (first)
         * t3: 1                 (second)
         * }
         */
        private Collection<QualifiedName> getOrderedRelationNames(MultiSourceSelect statement) {
            OrderBy orderBy = statement.querySpec().orderBy();
            if (orderBy == null || !orderBy.isSorted()) {
                return statement.sources().keySet();
            }
            final List<QualifiedName> orderByOrder = new ArrayList<>(statement.sources().size());
            for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
                for (Map.Entry<QualifiedName, MultiSourceSelect.Source> entry : statement.sources().entrySet()) {
                    OrderBy subOrderBy = entry.getValue().querySpec().orderBy();
                    if (subOrderBy == null || !subOrderBy.isSorted() || orderByOrder.contains(entry.getKey())) {
                        continue;
                    }
                    if (orderBySymbol.equals(subOrderBy.orderBySymbols().get(0))) {
                        orderByOrder.add(entry.getKey());
                        break;
                    }
                }
            }
            if (orderByOrder.size() < statement.sources().size()) {
                Iterator<QualifiedName> iter = statement.sources().keySet().iterator();
                while (orderByOrder.size() < statement.sources().size()) {
                    QualifiedName qn = iter.next();
                    if (!orderByOrder.contains(qn)) {
                        orderByOrder.add(qn);
                    }
                }
            }
            return orderByOrder;
        }

        private boolean isUnsupportedStatement(MultiSourceSelect statement, ConsumerContext context) {
            if (statement.sources().size() != 2) {
                context.validationException(new ValidationException("Joining more than 2 relations is not supported"));
                return true;
            }

            List<Symbol> groupBy = statement.querySpec().groupBy();
            if (groupBy != null && !groupBy.isEmpty()) {
                context.validationException(new ValidationException("GROUP BY on CROSS JOIN is not supported"));
                return true;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on CROSS JOIN is not supported"));
                return true;
            }

            if (hasOutputsToFetch(statement.querySpec())) {
                context.validationException(new ValidationException("Only fields that are used in ORDER BY can be selected within a CROSS JOIN"));
                return true;
            }
            return false;
        }

        private boolean hasOutputsToFetch(QuerySpec querySpec) {
            FetchRequiredVisitor.Context ctx = new FetchRequiredVisitor.Context(querySpec.orderBy());
            return FetchRequiredVisitor.INSTANCE.process(querySpec.outputs(), ctx);
        }
    }

    private static class SubRelationConverter extends AnalyzedRelationVisitor<MultiSourceSelect.Source, QueriedTableRelation> {

        static final SubRelationConverter INSTANCE = new SubRelationConverter();

        @Override
        public QueriedTableRelation visitTableRelation(TableRelation tableRelation,
                                                       MultiSourceSelect.Source source) {
            return new QueriedTable(tableRelation, source.querySpec());
        }

        @Override
        public QueriedTableRelation visitDocTableRelation(DocTableRelation tableRelation,
                                                          MultiSourceSelect.Source source) {
            return new QueriedDocTable(tableRelation, source.querySpec());
        }

        @Override
        protected QueriedTableRelation visitAnalyzedRelation(AnalyzedRelation relation,
                                                             MultiSourceSelect.Source source) {
            throw new ValidationException("CROSS JOIN with sub queries is not supported");
        }
    }

}
