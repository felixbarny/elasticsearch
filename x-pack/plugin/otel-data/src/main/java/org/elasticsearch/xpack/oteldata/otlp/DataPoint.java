/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface DataPoint {

    long getTimestampUnixNano();

    List<KeyValue> getAttributes();

    long getStartTimestampUnixNano();

    String getUnit();

    String getMetricName();

    void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException;

    String getDynamicTemplate(MappingHints mappingHints);

    boolean isValid(Set<String> messages);

    long getDocCount();

    record Number(NumberDataPoint dataPoint, Metric metric) implements DataPoint {

        @Override
        public long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        @Override
        public List<KeyValue> getAttributes() {
            return dataPoint.getAttributesList();
        }

        @Override
        public long getStartTimestampUnixNano() {
            return dataPoint.getStartTimeUnixNano();
        }

        @Override
        public String getUnit() {
            return metric.getUnit();
        }

        @Override
        public String getMetricName() {
            return metric.getName();
        }

        @Override
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException {
            switch (dataPoint.getValueCase()) {
                case AS_DOUBLE -> builder.value(dataPoint.getAsDouble());
                case AS_INT -> builder.value(dataPoint.getAsInt());
            }
        }

        @Override
        public long getDocCount() {
            return 1;
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            String prefix = metric.getDataCase() == Metric.DataCase.SUM ? "counter_" : "gauge_";
            if (dataPoint.getValueCase() == NumberDataPoint.ValueCase.AS_INT) {
                return prefix + "long";
            } else if (dataPoint.getValueCase() == NumberDataPoint.ValueCase.AS_DOUBLE) {
                return prefix + "double";
            } else {
                return null;
            }
        }

        @Override
        public boolean isValid(Set<String> messages) {
            return true;
        }
    }

    record ExponentialHistogram(ExponentialHistogramDataPoint dataPoint, Metric metric) implements DataPoint {

        @Override
        public long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        @Override
        public List<KeyValue> getAttributes() {
            return dataPoint.getAttributesList();
        }

        @Override
        public long getStartTimestampUnixNano() {
            return dataPoint.getStartTimeUnixNano();
        }

        @Override
        public String getUnit() {
            return metric.getUnit();
        }

        @Override
        public String getMetricName() {
            return metric.getName();
        }

        @Override
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException {
            if (mappingHints.aggregateMetricDouble()) {
                builder.startObject();
                builder.field("sum", dataPoint.getSum());
                builder.field("value_count", dataPoint.getCount());
                builder.endObject();
            } else {
                builder.startObject();
                builder.startArray("counts");
                HistogramConverter.counts(dataPoint, builder::value);
                builder.endArray();
                builder.startArray("values");
                HistogramConverter.centroidValues(dataPoint, builder::value);
                builder.endArray();
                builder.endObject();
            }
        }

        @Override
        public long getDocCount() {
            return dataPoint.getCount();
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            if (mappingHints.aggregateMetricDouble()) {
                return "summary";
            } else {
                return "histogram";
            }
        }

        @Override
        public boolean isValid(Set<String> messages) {
            boolean valid = metric.getExponentialHistogram()
                .getAggregationTemporality() == AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
            if (valid == false) {
                messages.add("cumulative exponential histogram metrics are not supported, ignoring " + metric.getName());
            }
            return valid;
        }
    }

    record Histogram(HistogramDataPoint dataPoint, Metric metric) implements DataPoint {
        @Override
        public long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        @Override
        public List<KeyValue> getAttributes() {
            return dataPoint.getAttributesList();
        }

        @Override
        public long getStartTimestampUnixNano() {
            return dataPoint.getStartTimeUnixNano();
        }

        @Override
        public String getUnit() {
            return metric.getUnit();
        }

        @Override
        public String getMetricName() {
            return metric.getName();
        }

        @Override
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException {
            if (mappingHints.aggregateMetricDouble()) {
                builder.startObject();
                builder.field("sum", dataPoint.getSum());
                builder.field("value_count", dataPoint.getCount());
                builder.endObject();
            } else {
                builder.startObject();
                builder.startArray("counts");
                HistogramConverter.counts(dataPoint, builder::value);
                builder.endArray();
                builder.startArray("values");
                HistogramConverter.centroidValues(dataPoint, builder::value);
                builder.endArray();
                builder.endObject();
            }
        }

        @Override
        public long getDocCount() {
            return dataPoint.getCount();
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            if (mappingHints.aggregateMetricDouble()) {
                return "summary";
            } else {
                return "histogram";
            }
        }

        @Override
        public boolean isValid(Set<String> messages) {
            boolean valid = metric.getHistogram().getAggregationTemporality() == AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
            if (valid == false) {
                messages.add("cumulative histogram metrics are not supported, ignoring " + metric.getName());
            }
            return valid;
        }
    }

    record Summary(SummaryDataPoint dataPoint, Metric metric) implements DataPoint {

        @Override
        public long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        @Override
        public List<KeyValue> getAttributes() {
            return dataPoint.getAttributesList();
        }

        @Override
        public long getStartTimestampUnixNano() {
            return dataPoint.getStartTimeUnixNano();
        }

        @Override
        public String getUnit() {
            return metric.getUnit();
        }

        @Override
        public String getMetricName() {
            return metric.getName();
        }

        @Override
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException {
            // TODO: Add support for quantiles
            builder.startObject();
            builder.field("sum", dataPoint.getSum());
            builder.field("value_count", dataPoint.getCount());
            builder.endObject();
        }

        @Override
        public long getDocCount() {
            return dataPoint.getCount();
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            return "summary";
        }

        @Override
        public boolean isValid(Set<String> messages) {
            return true;
        }
    }
}
