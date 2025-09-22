/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.predicate.operator.aggregation;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.core.util.CollectionUtils.combine;

public abstract class VectorAggregation extends Function {

    public enum Grouping {
        BY,
        WITHOUT,
        NONE
    }

    private final Expression field;
    private final List<Expression> parameters;
    private final Grouping grouping;
    private final Set<String> labels;
    private final String name;

    protected VectorAggregation(
        Source source,
        Expression field,
        List<Expression> parameters,
        Grouping grouping,
        Set<String> labels,
        String name
    ) {
        super(source, combine(singletonList(field), parameters));
        this.field = field;
        this.parameters = parameters;
        this.grouping = grouping;
        this.labels = labels;
        this.name = name;
    }

    public Expression field() {
        return field;
    }

    public List<Expression> parameters() {
        return parameters;
    }

    public Grouping grouping() {
        return grouping;
    }

    public Set<String> labels() {
        return labels;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && Expressions.foldable(parameters);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new VectorAggregation(source(), newChildren.get(0), parameters(), grouping(), labels(), null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, VectorAggregation::new, field(), parameters(), grouping(), labels(), null);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            VectorAggregation that = (VectorAggregation) o;
            return grouping == that.grouping && Objects.equals(name, that.name) && Objects.equals(labels, that.labels);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name, grouping, labels);
    }
}
