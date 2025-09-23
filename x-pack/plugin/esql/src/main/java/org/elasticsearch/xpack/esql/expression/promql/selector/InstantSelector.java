/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.selector;

import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.promql.types.PromqlDataTypes;

import java.util.List;

public class InstantSelector extends Selector {

    public InstantSelector(Source source, List<LabelMatcher> labels, Evaluation evaluation) {
        super(source, labels, evaluation);
    }

    @Override
    protected NodeInfo<InstantSelector> info() {
        return NodeInfo.create(this, InstantSelector::new, labels(), evaluation());
    }

    @Override
    public DataType dataType() {
        return PromqlDataTypes.INSTANT_VECTOR;
    }
}
