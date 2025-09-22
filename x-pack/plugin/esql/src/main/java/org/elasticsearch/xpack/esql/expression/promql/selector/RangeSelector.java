/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.selector;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Objects;

public class RangeSelector extends Selector {
    private final TimeValue range;

    public RangeSelector(Source source, List<LabelMatcher> labels, TimeValue range, Evaluation evaluation) {
        super(source, labels, evaluation);
        this.range = range;
    }

    public TimeValue range() {
        return range;
    }

    @Override
    public DataType dataType() {
        return PromqlDataTypes.RANGE_VECTOR;
    }

    @Override
    protected NodeInfo<RangeSelector> info() {
        return NodeInfo.create(this, RangeSelector::new, labels(), range, evaluation());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        RangeSelector that = (RangeSelector) o;
        return Objects.equals(range, that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), range);
    }
}
