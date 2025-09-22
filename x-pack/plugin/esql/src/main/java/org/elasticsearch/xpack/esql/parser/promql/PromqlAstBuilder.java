/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.parser.ExpressionBuilder;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser;

import java.time.Instant;

public class PromqlAstBuilder extends ExpressionBuilder {

    PromqlAstBuilder(Instant start, Instant stop) {
        super(start, stop);
    }

    @Override
    public Expression visitSingleExpression(PromqlBaseParser.SingleExpressionContext ctx) {
        return super.visitSingleExpression(ctx);
    }
}
