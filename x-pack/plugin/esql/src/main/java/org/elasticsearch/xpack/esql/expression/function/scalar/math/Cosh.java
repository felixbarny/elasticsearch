/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

/**
 * Cosine hyperbolic function.
 */
public class Cosh extends AbstractTrigonometricFunction {
    @FunctionInfo(
        returnType = "double",
        description = "Returns the {wikipedia}/Hyperbolic_functions[hyperbolic cosine] of an angle.",
        examples = @Example(file = "floats", tag = "cosh")
    )
    public Cosh(
        Source source,
        @Param(
            name = "angle",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "An angle, in radians. If `null`, the function returns `null`."
        ) Expression angle
    ) {
        super(source, angle);
    }

    @Override
    protected EvalOperator.ExpressionEvaluator.Factory doubleEvaluator(EvalOperator.ExpressionEvaluator.Factory field) {
        return new CoshEvaluator.Factory(source(), field);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Cosh(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Cosh::new, field());
    }

    @Evaluator(warnExceptions = ArithmeticException.class)
    static double process(double val) {
        double res = Math.cosh(val);
        if (Double.isNaN(res) || Double.isInfinite(res)) {
            throw new ArithmeticException("cosh overflow");
        }
        return res;
    }
}
