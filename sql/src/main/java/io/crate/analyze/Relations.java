/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.metadata.OutputName;
import io.crate.metadata.Path;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class Relations {

    static QueriedRelation upgrade(AnalyzedRelation relation, QuerySpec querySpec) {
        if (relation instanceof DocTableRelation) {
            return new QueriedDocTable((DocTableRelation) relation, namesFromOutputs(querySpec.outputs()), querySpec);
        }
        if (relation instanceof TableRelation) {
            return new QueriedTable((TableRelation) relation, namesFromOutputs(querySpec.outputs()), querySpec);
        }
        return new QueriedSelectRelation(((QueriedRelation) relation), relation.fields(), querySpec);
    }

    private static Collection<? extends Path> namesFromOutputs(List<Symbol> outputs) {
        List<Path> outputNames = new ArrayList<>(outputs.size());
        for (Symbol output : outputs) {
            if (output instanceof Field) {
                outputNames.add(((Field) output).path());
            } else {
                outputNames.add(new OutputName(SymbolPrinter.INSTANCE.printSimple(output)));
            }
        }
        return outputNames;
    }
}
