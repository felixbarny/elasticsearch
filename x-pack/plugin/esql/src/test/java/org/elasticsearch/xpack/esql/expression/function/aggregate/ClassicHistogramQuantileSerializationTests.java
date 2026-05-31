/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class ClassicHistogramQuantileSerializationTests extends AbstractExpressionSerializationTests<ClassicHistogramQuantile> {
    @Override
    protected ClassicHistogramQuantile createTestInstance() {
        return new ClassicHistogramQuantile(randomSource(), randomChild(), randomChild(), randomChild());
    }

    @Override
    protected ClassicHistogramQuantile mutateInstance(ClassicHistogramQuantile instance) throws IOException {
        Expression field = instance.field();
        Expression upperBound = instance.upperBound();
        Expression quantile = instance.quantile();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> upperBound = randomValueOtherThan(upperBound, AbstractExpressionSerializationTests::randomChild);
            default -> quantile = randomValueOtherThan(quantile, AbstractExpressionSerializationTests::randomChild);
        }
        return new ClassicHistogramQuantile(instance.source(), field, upperBound, quantile);
    }
}
