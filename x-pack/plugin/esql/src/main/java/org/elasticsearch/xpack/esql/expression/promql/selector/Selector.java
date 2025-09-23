/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.selector;

import org.apache.http.protocol.ExecutionContext;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.expression.LeafExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public abstract class Selector extends LeafExpression {

    private final List<LabelMatcher> labels;
    private final Evaluation evaluation;

    Selector(Source source, List<LabelMatcher> labels, Evaluation evaluation) {
        super(source);
        this.labels = labels;
        this.evaluation = evaluation;
    }

    public List<LabelMatcher> labels() {
        return labels;
    }

    public Evaluation evaluation() {
        return evaluation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Selector selector = (Selector) o;
        return Objects.equals(labels, selector.labels) && Objects.equals(evaluation, selector.evaluation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), labels, evaluation);
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public final boolean foldable() {
        return false;
    }


    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("should not serialize");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("should not serialize");
    }
}
