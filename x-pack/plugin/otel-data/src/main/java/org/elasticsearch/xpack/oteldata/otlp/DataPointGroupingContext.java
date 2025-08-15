/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import com.dynatrace.hash4j.hashing.HashStream32;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher32;
import com.dynatrace.hash4j.hashing.Hashing;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.tsid.DataPointTsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.tsid.ResourceTsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.tsid.ScopeTsidFunnel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class DataPointGroupingContext {
    private static final Hasher32 HASHER_32 = Hashing.murmur3_32();

    // <resourceHash, <scopeHash, <dataPointDimensionsHash, List<DataPoint>>>
    private final Map<HashValue128, Map<HashValue128, Map<HashValue128, DataPointGroup>>> dataPoints = new HashMap<>();

    private int totalDataPoints = 0;
    private int ignoredDataPoints = 0;
    private final Set<String> ignoredDataPointMessages = new HashSet<>();

    public void groupDataPoints(ExportMetricsServiceRequest exportMetricsServiceRequest) {
        List<ResourceMetrics> resourceMetricsList = exportMetricsServiceRequest.getResourceMetricsList();
        for (int i = 0; i < resourceMetricsList.size(); i++) {
            ResourceMetrics resourceMetrics = resourceMetricsList.get(i);
            TsidBuilder resourceTsidBuilder = ResourceTsidFunnel.forResource(resourceMetrics);
            HashValue128 resourceHash = resourceTsidBuilder.hash();
            List<ScopeMetrics> scopeMetricsList = resourceMetrics.getScopeMetricsList();
            for (int j = 0; j < scopeMetricsList.size(); j++) {
                ScopeMetrics scopeMetrics = scopeMetricsList.get(j);
                InstrumentationScope scope = scopeMetrics.getScope();
                TsidBuilder scopeTsidBuilder = ScopeTsidFunnel.forScope(scopeMetrics);
                HashValue128 scopeHash = scopeTsidBuilder.hash();
                List<Metric> metricsList = scopeMetrics.getMetricsList();
                for (int k = 0; k < metricsList.size(); k++) {
                    var metric = metricsList.get(k);
                    // TODO: add support for other metric types
                    switch (metric.getDataCase()) {
                        case SUM -> addDataPoints(
                            resourceHash,
                            resourceMetrics.getResource(),
                            resourceMetrics.getSchemaUrl(),
                            resourceTsidBuilder,
                            scopeHash,
                            scopeTsidBuilder,
                            scope,
                            scopeMetrics.getSchemaUrl(),
                            metric, metric.getSum().getDataPointsList(),
                            DataPoint.Number::new
                        );
                        case GAUGE -> addDataPoints(
                            resourceHash,
                            resourceMetrics.getResource(),
                            resourceMetrics.getSchemaUrl(),
                            resourceTsidBuilder,
                            scopeHash,
                            scopeTsidBuilder,
                            scope,
                            scopeMetrics.getSchemaUrl(),
                            metric, metric.getGauge().getDataPointsList(),
                            DataPoint.Number::new
                        );
                        case EXPONENTIAL_HISTOGRAM -> addDataPoints(
                            resourceHash,
                            resourceMetrics.getResource(),
                            resourceMetrics.getSchemaUrl(),
                            resourceTsidBuilder,
                            scopeHash,
                            scopeTsidBuilder,
                            scope,
                            scopeMetrics.getSchemaUrl(),
                            metric, metric.getExponentialHistogram().getDataPointsList(),
                            DataPoint.ExponentialHistogram::new
                        );
                        case HISTOGRAM -> addDataPoints(
                            resourceHash,
                            resourceMetrics.getResource(),
                            resourceMetrics.getSchemaUrl(),
                            resourceTsidBuilder,
                            scopeHash,
                            scopeTsidBuilder,
                            scope,
                            scopeMetrics.getSchemaUrl(),
                            metric, metric.getHistogram().getDataPointsList(),
                            DataPoint.Histogram::new
                        );
                        case SUMMARY -> {
                            totalDataPoints += metric.getSummary().getDataPointsCount();
                            ignoredDataPoints += metric.getSummary().getDataPointsCount();
                            ignoredDataPointMessages.add("summary metrics are not supported, ignoring " + metric.getName());
                        }
                        default -> {
                            ignoredDataPoints++;
                            ignoredDataPointMessages.add("unknown metric type " + metric.getDataCase());
                        }
                    }
                }
            }
        }
    }

    public <T> void addDataPoints(
        HashValue128 resourceHash,
        Resource resource,
        String resourceSchemaUrl,
        TsidBuilder resourceTsidBuilder,
        HashValue128 scopeHash,
        TsidBuilder scopeTsidBuilder,
        InstrumentationScope scope,
        String scopeSchemaUrl,
        Metric metric, List<T> dataPoints,
        BiFunction<T, Metric, DataPoint> dataPointConverter
    ) {
        for (int i = 0; i < dataPoints.size(); i++) {
            T dataPoint = dataPoints.get(i);
            addDataPoint(
                resourceHash,
                resource,
                resourceSchemaUrl,
                resourceTsidBuilder,
                scopeHash,
                scopeTsidBuilder,
                scope,
                scopeSchemaUrl,
                dataPointConverter.apply(dataPoint, metric)
            );
        }
        totalDataPoints += dataPoints.size();
    }

    public void addDataPoint(
        HashValue128 resourceHash,
        Resource resource,
        String resourceSchemaUrl,
        TsidBuilder resourceTsidBuilder,
        HashValue128 scopeHash,
        TsidBuilder scopeTsidBuilder,
        InstrumentationScope scope,
        String scopeSchemaUrl,
        DataPoint dataPoint
    ) {
        if (dataPoint.isValid(ignoredDataPointMessages) == false) {
            ignoredDataPoints++;
            return;
        }
        TsidBuilder dataPointGroupTsidBuilder = DataPointTsidFunnel.forDataPoint(dataPoint);
        HashValue128 dataPointGroupHash = dataPointGroupTsidBuilder.hash();
        DataPointGroup dataPointGroup = dataPoints.computeIfAbsent(resourceHash, k -> new HashMap<>())
            .computeIfAbsent(scopeHash, k -> new HashMap<>())
            .computeIfAbsent(
                dataPointGroupHash,
                k -> new DataPointGroup(
                    resource,
                    resourceSchemaUrl,
                    resourceHash.toString(),
                    resourceTsidBuilder,
                    scope,
                    scopeSchemaUrl,
                    scopeHash.toString(),
                    scopeTsidBuilder,
                    dataPointGroupHash.toString(),
                    dataPointGroupTsidBuilder,
                    dataPoint.getAttributes(),
                    dataPoint.getUnit(),
                    new ArrayList<>(),
                    TargetIndex.route(
                        "metrics",
                        dataPoint.getAttributes(),
                        scope.getName(),
                        scope.getAttributesList(),
                        resource.getAttributesList()
                    )
                )
            );
        dataPointGroup.addDataPoint(dataPoint);
    }

    public <E extends Exception> void forEach(CheckedConsumer<DataPointGroup, E> consumer) throws E {
        for (Map.Entry<HashValue128, Map<HashValue128, Map<HashValue128, DataPointGroup>>> entry : dataPoints.entrySet()) {
            for (Map.Entry<HashValue128, Map<HashValue128, DataPointGroup>> entry2 : entry.getValue().entrySet()) {
                for (Map.Entry<HashValue128, DataPointGroup> entry3 : entry2.getValue().entrySet()) {
                    consumer.accept(entry3.getValue());
                }
            }
        }
    }

    public int totalDataPoints() {
        return totalDataPoints;
    }

    public int getIgnoredDataPoints() {
        return ignoredDataPoints;
    }

    public String getIgnoredDataPointsMessage() {
        return ignoredDataPointMessages.isEmpty() ? "" : String.join("\n", ignoredDataPointMessages);
    }

    public interface DataPoint {
        long getTimestampUnixNano();

        List<KeyValue> getAttributes();

        long getStartTimestampUnixNano();

        String getUnit();

        String getMetricName();

        void buildMetricValue(XContentBuilder builder) throws IOException;

        String getDynamicTemplate();

        boolean isValid(Set<String> messages);

        record Number(NumberDataPoint dataPoint, Metric metric) implements DataPoint {

            @Override
            public long getTimestampUnixNano() {
                return dataPoint.getTimeUnixNano();
            }

            @Override
            public List<KeyValue> getAttributes() {
                return dataPoint().getAttributesList();
            }

            @Override
            public long getStartTimestampUnixNano() {
                return dataPoint.getStartTimeUnixNano();
            }

            @Override
            public String getUnit() {
                return metric().getUnit();
            }

            @Override
            public String getMetricName() {
                return metric.getName();
            }

            @Override
            public void buildMetricValue(XContentBuilder builder) throws IOException {
                switch (dataPoint.getValueCase()) {
                    case AS_DOUBLE -> builder.value(dataPoint.getAsDouble());
                    case AS_INT -> builder.value(dataPoint.getAsInt());
                }
            }

            @Override
            public String getDynamicTemplate() {
                String prefix = metric.getDataCase() == Metric.DataCase.SUM ? "counter_" : "gauge_";
                return switch (dataPoint.getValueCase()) {
                    case AS_INT -> prefix + "long";
                    case AS_DOUBLE -> prefix + "double";
                    case VALUE_NOT_SET -> null;
                };
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
                return dataPoint().getAttributesList();
            }

            @Override
            public long getStartTimestampUnixNano() {
                return dataPoint.getStartTimeUnixNano();
            }

            @Override
            public String getUnit() {
                return metric().getUnit();
            }

            @Override
            public String getMetricName() {
                return metric.getName();
            }

            @Override
            public void buildMetricValue(XContentBuilder builder) throws IOException {
                builder.startObject();
                builder.startArray("counts");
                HistogramConverter.counts(dataPoint, builder::value);
                builder.endArray();
                builder.startArray("values");
                HistogramConverter.centroidValues(dataPoint, builder::value);
                builder.endArray();
                builder.endObject();
            }

            @Override
            public String getDynamicTemplate() {
                return "histogram";
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
                return dataPoint().getAttributesList();
            }

            @Override
            public long getStartTimestampUnixNano() {
                return dataPoint.getStartTimeUnixNano();
            }

            @Override
            public String getUnit() {
                return metric().getUnit();
            }

            @Override
            public String getMetricName() {
                return metric.getName();
            }

            @Override
            public void buildMetricValue(XContentBuilder builder) throws IOException {
                builder.startObject();
                builder.startArray("counts");
                HistogramConverter.counts(dataPoint, builder::value);
                builder.endArray();
                builder.startArray("values");
                HistogramConverter.centroidValues(dataPoint, builder::value);
                builder.endArray();
                builder.endObject();
            }

            @Override
            public String getDynamicTemplate() {
                return "histogram";
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
    }

    public record DataPointGroup(
        Resource resource,
        String resourceSchemaUrl,
        String resourceHash,
        TsidBuilder resourceTsidBuilder,
        InstrumentationScope scope,
        String scopeSchemaUrl,
        String scopeHash,
        TsidBuilder scopeTsidBuilder,
        String dataPointGroupHash,
        TsidBuilder dataPointGroupTsidBuilder,
        List<KeyValue> dataPointAttributes,
        String unit,
        List<DataPoint> dataPoints,
        TargetIndex targetIndex
    ) {

        public long getTimestampUnixNano() {
            return dataPoints.getFirst().getTimestampUnixNano();
        }

        public long getStartTimestampUnixNano() {
            return dataPoints.getFirst().getStartTimestampUnixNano();
        }

        public String getMetricNamesHash() {
            HashStream32 metricNamesHash = HASHER_32.hashStream();
            for (int i = 0; i < dataPoints.size(); i++) {
                metricNamesHash.putString(dataPoints.get(i).getMetricName());
            }
            return Integer.toHexString(metricNamesHash.getAsInt());
        }

        public void addDataPoint(DataPoint dataPoint) {
            dataPoints.add(dataPoint);
        }
    }
}
