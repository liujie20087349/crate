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

package io.crate.analyze;

import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FieldReplacer;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;

public class MultiSourceSelect implements QueriedRelation {

    private final RelationSplitter splitter;
    private final Map<QualifiedName, AnalyzedRelation> sources;
    private QuerySpec querySpec;
    private final Fields fields;
    private final List<JoinPair> joinPairs;
    private QualifiedName qualifiedName;
    private Set<Field> canBeFetched = Collections.emptySet();
    private Set<Symbol> requiredForQuery = Collections.emptySet();

    public MultiSourceSelect(Map<QualifiedName, AnalyzedRelation> sources,
                             Collection<? extends Path> outputNames,
                             QuerySpec querySpec,
                             List<JoinPair> joinPairs) {
        assert sources.size() > 1 : "MultiSourceSelect requires at least 2 relations";
        this.splitter = new RelationSplitter(querySpec, sources.values(), joinPairs);
        this.sources = sources;
        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : sources.entrySet()) {
            entry.getValue().setQualifiedName(entry.getKey());
        }
        this.querySpec = querySpec;
        this.joinPairs = joinPairs;
        assert outputNames.size() == querySpec.outputs().size() : "size of outputNames and outputSymbols must match";
        fields = new Fields(outputNames.size());
        Iterator<Symbol> outputsIterator = querySpec.outputs().iterator();
        for (Path path : outputNames) {
            fields.add(path, new Field(this, path, outputsIterator.next().valueType()));
        }
    }

    public MultiSourceSelect(MultiSourceSelect mss, QuerySpec querySpec) {
        this.sources = mss.sources;
        this.joinPairs = mss.joinPairs;
        this.splitter = new RelationSplitter(
            querySpec,
            sources.values(),
            joinPairs);
        this.querySpec = querySpec;
        this.fields = mss.fields;
    }

    public Set<Symbol> requiredForQuery() {
        return requiredForQuery;
    }

    public Set<Field> canBeFetched() {
        return canBeFetched;
    }

    public Map<QualifiedName, AnalyzedRelation> sources() {
        return sources;
    }

    public List<JoinPair> joinPairs() {
        return joinPairs;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitMultiSourceSelect(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on MultiSourceSelect is only supported for READ operations");
        }
        return fields.get(path);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    @Override
    public QuerySpec querySpec() {
        return querySpec;
    }

    public void pushDownQuerySpecs() {
        // FIXME: instead of mutating everything, create a new MSS
        splitter.process();
        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : sources.entrySet()) {
            AnalyzedRelation relation = entry.getValue();
            QuerySpec spec = splitter.getSpec(relation);
            QueriedRelation queriedRelation = Relations.upgrade(relation, spec);
            queriedRelation.setQualifiedName(relation.getQualifiedName());
            Function<Field, Field> reResolveFieldFromUpgradedRelation = f -> {
                if (f.relation() == relation) {
                    return queriedRelation.getField(f.path(), Operation.READ);
                }
                return f;
            };
            querySpec = querySpec.copyAndReplace(FieldReplacer.bind(reResolveFieldFromUpgradedRelation));
            entry.setValue(queriedRelation);
        }
        canBeFetched = splitter.canBeFetched();
        requiredForQuery = splitter.requiredForQuery();
    }

    public Optional<RemainingOrderBy> remainingOrderBy() {
        return splitter.remainingOrderBy();
    }
}
