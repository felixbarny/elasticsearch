/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorMatch;

import java.util.Objects;

public class VectorBinaryComparison extends VectorBinaryOperator {

    public enum ComparisonOp implements BinaryOp {
        EQ,
        NEQ,
        GT,
        GTE,
        LT,
        LTE;

        @Override
        public Function asFunction() {
            throw new UnsupportedOperationException("not implemented");
        }
    }

    private final ComparisonOp op;
    private final boolean boolMode;

    public VectorBinaryComparison(Source source, Expression left, Expression right, VectorMatch match, boolean boolMode, ComparisonOp op) {
        super(source, left, right, match, boolMode == false, op);
        this.op = op;
        this.boolMode = boolMode;
    }

    public ComparisonOp op() {
        return op;
    }

    public boolean boolMode() {
        return boolMode;
    }

    @Override
    protected VectorBinaryOperator replaceChildren(Expression left, Expression right) {
        return new VectorBinaryComparison(source(), left, right, match(), boolMode, op());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, VectorBinaryComparison::new, left(), right(), match(), boolMode(), op());
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            VectorBinaryComparison that = (VectorBinaryComparison) o;
            return boolMode == that.boolMode;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), boolMode);
    }
}
