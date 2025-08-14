/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;

import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;

class ExponentialHistogramConverter {

    public static <E extends Exception> void counts(ExponentialHistogramDataPoint dp, CheckedLongConsumer<E> consumer) throws E {
        ExponentialHistogramDataPoint.Buckets negative = dp.getNegative();

        for (int i = negative.getBucketCountsCount() - 1; i >= 0; i--) {
            long count = negative.getBucketCounts(i);
            if (count == 0) {
                continue;
            }
            consumer.accept(count);
        }

        long zeroCount = dp.getZeroCount();
        if (zeroCount > 0) {
            consumer.accept(zeroCount);
        }

        ExponentialHistogramDataPoint.Buckets positive = dp.getPositive();
        for (int i = 0; i < positive.getBucketCountsCount(); i++) {
            long count = positive.getBucketCounts(i);
            if (count == 0) {
                continue;
            }
            consumer.accept(count);
        }
    }

    public static <E extends Exception> void centroidValues(ExponentialHistogramDataPoint dp, CheckedDoubleConsumer<E> consumer) throws E {
        int scale = dp.getScale();
        ExponentialHistogramDataPoint.Buckets negative = dp.getNegative();

        int offset = negative.getOffset();
        for (int i = negative.getBucketCountsCount() - 1; i >= 0; i--) {
            long count = negative.getBucketCounts(i);
            if (count == 0) {
                continue;
            }
            double lb = -ExponentialScaleUtils.getLowerBucketBoundary(offset + i, scale);
            double ub = -ExponentialScaleUtils.getUpperBucketBoundary(offset + i, scale);
            consumer.accept(lb + (ub - lb) / 2);
        }

        long zeroCount = dp.getZeroCount();
        if (zeroCount > 0) {
            consumer.accept(0.0);
        }

        ExponentialHistogramDataPoint.Buckets positive = dp.getPositive();
        offset = positive.getOffset();
        for (int i = 0; i < positive.getBucketCountsCount(); i++) {
            long count = positive.getBucketCounts(i);
            if (count == 0) {
                continue;
            }
            double lb = ExponentialScaleUtils.getLowerBucketBoundary(offset + i, scale);
            double ub = ExponentialScaleUtils.getUpperBucketBoundary(offset + i, scale);
            consumer.accept(lb + (ub - lb) / 2);
        }
    }

    interface CheckedLongConsumer<E extends Exception> {
        void accept(long value) throws E;
    }

    interface CheckedDoubleConsumer<E extends Exception> {
        void accept(double value) throws E;
    }
}
