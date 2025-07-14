/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.dynatrace.hash4j.hashing.HashStream32;
import com.dynatrace.hash4j.hashing.Hasher32;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher128;
import com.dynatrace.hash4j.hashing.Hashing;
import com.google.protobuf.MessageLite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.cbor.CborXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class OTLPMetricsTransportAction extends HandledTransportAction<
    OTLPMetricsTransportAction.MetricsRequest,
    OTLPMetricsTransportAction.MetricsResponse> {

    public static final String NAME = "indices:data/write/metrics";
    public static final ActionType<OTLPMetricsTransportAction.MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(OTLPMetricsTransportAction.class);
    private static final Hasher128 HASHER_128 = Hashing.murmur3_128();
    private static final Hasher32 HASHER_32 = Hashing.murmur3_32();
    private final Client client;

    @Inject
    public OTLPMetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client
    ) {
        super(NAME, transportService, actionFilters, MetricsRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, MetricsRequest request, ActionListener<MetricsResponse> listener) {
        try {
            var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.exportMetricsServiceRequest.streamInput());
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            addIndexRequests(bulkRequestBuilder, metricsServiceRequest);
            bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    MessageLite response;
                    if (bulkItemResponses.hasFailures()) {
                        long failures = Arrays.stream(bulkItemResponses.getItems()).filter(BulkItemResponse::isFailed).count();
                        response = ExportMetricsServiceResponse.newBuilder()
                            .getPartialSuccessBuilder()
                            .setRejectedDataPoints(failures)
                            .setErrorMessage(bulkItemResponses.buildFailureMessage())
                            .build();
                        logger.debug("OTLP request completed with failures {}", bulkItemResponses.buildFailureMessage());
                    } else {
                        response = ExportMetricsServiceResponse.newBuilder().build();
                    }
                    listener.onResponse(new MetricsResponse(response));
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(e.getMessage(), e);
                    listener.onFailure(e);
                }
            });

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            listener.onFailure(e);
        }
    }

    private void addIndexRequests(BulkRequestBuilder bulkRequestBuilder, ExportMetricsServiceRequest exportMetricsServiceRequest)
        throws IOException {
        Set<String> fragmentIds = new HashSet<>();
        List<ResourceMetrics> resourceMetricsList = exportMetricsServiceRequest.getResourceMetricsList();
        for (int i = 0; i < resourceMetricsList.size(); i++) {
            ResourceMetrics resourceMetrics = resourceMetricsList.get(i);
            List<KeyValue> resourceAttributes = resourceMetrics.getResource().getAttributesList();
            HashValue128 resourceAttributesHash = HASHER_128.hashTo128Bits(resourceAttributes, AttributeListHashFunnel.get());
            // Create a fragment for resource attributes to avoid repetition in each document
            String fragmentId = resourceAttributesHash.toString();
            addResourceFragment(fragmentIds, bulkRequestBuilder, resourceAttributes, fragmentId);
            List<ScopeMetrics> scopeMetricsList = resourceMetrics.getScopeMetricsList();
            for (int j = 0; j < scopeMetricsList.size(); j++) {
                ScopeMetrics scopeMetrics = scopeMetricsList.get(j);
                List<KeyValue> scopeAttributes = scopeMetrics.getScope().getAttributesList();
                HashValue128 scopeAttributesHash = HASHER_128.hashTo128Bits(scopeAttributes, AttributeListHashFunnel.get());
                List<Metric> metricsList = scopeMetrics.getMetricsList();
                Map<HashValue128, List<DataPoint>> dataPoints = new HashMap<>();
                for (int k = 0; k < metricsList.size(); k++) {
                    var metric = metricsList.get(k);
                    switch (metric.getDataCase()) {
                        case SUM -> {
                            List<NumberDataPoint> dataPointsList = metric.getSum().getDataPointsList();
                            groupNumberDataPoints(dataPoints, metric, dataPointsList);
                        }
                        case GAUGE -> {
                            List<NumberDataPoint> dataPointsList = metric.getGauge().getDataPointsList();
                            groupNumberDataPoints(dataPoints, metric, dataPointsList);
                        }
                        default -> throw new IllegalArgumentException("Unsupported metric type [" + metric.getDataCase() + "]");
                    }
                }
                createNumberDataPointDocs(
                    resourceAttributes,
                    resourceAttributesHash,
                    scopeAttributes,
                    scopeAttributesHash,
                    dataPoints,
                    bulkRequestBuilder
                );
            }
        }
    }

    private void addResourceFragment(
        Set<String> fragmentIds,
        BulkRequestBuilder bulkRequestBuilder,
        List<KeyValue> resourceAttributes,
        String fragmentId
    ) throws IOException {
        if (fragmentIds.add(fragmentId) == false) {
            return;
        }
        try (XContentBuilder resourceBuilder = CborXContent.contentBuilder()) {
            resourceBuilder.startObject();
            resourceBuilder.startObject("resource");
            resourceBuilder.startObject("attributes");
            buildAttributes(resourceBuilder, resourceAttributes);
            resourceBuilder.endObject();
            resourceBuilder.endObject();
            resourceBuilder.endObject();

            // Add the resource attributes fragment to the bulk request
            bulkRequestBuilder.add(client.prepareFragment("metrics-generic.otel-default").setId(fragmentId).setSource(resourceBuilder));
        }
    }

    private void groupNumberDataPoints(Map<HashValue128, List<DataPoint>> dataPoints, Metric metric, List<NumberDataPoint> dataPointsList) {
        for (int l = 0; l < dataPointsList.size(); l++) {
            DataPoint dataPoint = new DataPoint(dataPointsList.get(l), metric);
            HashValue128 hash = HASHER_128.hashTo128Bits(dataPoint, DataPointHashFunnel.get());
            dataPoints.computeIfAbsent(hash, h -> new ArrayList<>()).add(dataPoint);
        }
    }

    private void createNumberDataPointDocs(
        List<KeyValue> resourceAttributes,
        HashValue128 resourceAttributesHash,
        List<KeyValue> scopeAttributes,
        HashValue128 scopeAttributesHash,
        Map<HashValue128, List<DataPoint>> dataPoints,
        BulkRequestBuilder bulkRequestBuilder
    ) throws IOException {
        for (Map.Entry<HashValue128, List<DataPoint>> entry : dataPoints.entrySet()) {
            try (XContentBuilder xContentBuilder = CborXContent.contentBuilder()) {
                buildDataPointDoc(
                    xContentBuilder,
                    resourceAttributesHash,
                    scopeAttributes,
                    scopeAttributesHash,
                    entry.getValue()
                );
                bulkRequestBuilder.add(
                    client.prepareIndex("metrics-generic.otel-default")
                        .setCreate(true)
                        .setRequireDataStream(true)
                        .setPipeline(IngestService.NOOP_PIPELINE_NAME)
                        .setSource(xContentBuilder)
                        .addFragmentId(resourceAttributesHash.toString()) // Reference the resource attributes fragment
                );
            }
        }
    }

    private void buildDataPointDoc(
        XContentBuilder builder,
        HashValue128 resourceAttributesHash,
        List<KeyValue> scopeAttributes,
        HashValue128 scopeAttributesHash,
        List<DataPoint> dataPoints
    ) throws IOException {
        builder.startObject();
        DataPoint first = dataPoints.getFirst();
        builder.field("@timestamp", TimeUnit.NANOSECONDS.toMillis(first.getTimestampUnixNano()));
        if (first.getStartTimestampUnixNano() != 0) {
            builder.field("start_timestamp", TimeUnit.NANOSECONDS.toMillis(first.getStartTimestampUnixNano()));
        }
        builder.startObject("data_stream");
        builder.field("type", "metrics");
        builder.field("dataset", "generic.otel-default");
        builder.field("namespace", "default");
        builder.endObject();
        builder.startObject("scope");
        builder.startObject("attributes");
        buildAttributes(builder, scopeAttributes);
        builder.endObject();
        builder.endObject();
        builder.startObject("attributes");
        buildAttributes(builder, first.getAttributes());
        builder.endObject();
        builder.field("unit", first.getUnit());
        HashStream32 metricNamesHash = HASHER_32.hashStream();
        dataPoints.stream().map(DataPoint::getMetricName).forEach(metricNamesHash::putString);
        // TODO remove hack to maintain single writer
        builder.field("_metric_names_hash", Integer.toHexString(metricNamesHash.getAsInt()) + resourceAttributesHash.toString());
        builder.startObject("metrics");
        for (int i = 0; i < dataPoints.size(); i++) {
            NumberDataPoint dp = dataPoints.get(i).dataPoint();
            switch (dp.getValueCase()) {
                case AS_DOUBLE -> builder.field(dataPoints.get(i).getMetricName(), dp.getAsDouble());
                case AS_INT -> builder.field(dataPoints.get(i).getMetricName(), dp.getAsInt());
            }
        }
        builder.endObject();
        builder.endObject();
    }

    private void buildAttributes(XContentBuilder builder, List<KeyValue> resourceAttributes) throws IOException {
        for (KeyValue attribute : resourceAttributes) {

            builder.field(attribute.getKey());
            attributeValue(builder, attribute.getValue());
        }
    }

    private void attributeValue(XContentBuilder builder, AnyValue value) throws IOException {
        switch (value.getValueCase()) {
            case STRING_VALUE -> builder.value(value.getStringValue());
            case BOOL_VALUE -> builder.value(value.getBoolValue());
            case INT_VALUE -> builder.value(value.getIntValue());
            case DOUBLE_VALUE -> builder.value(value.getDoubleValue());
            case ARRAY_VALUE -> {
                builder.startArray();
                for (AnyValue arrayValue : value.getArrayValue().getValuesList()) {
                    attributeValue(builder, arrayValue);
                }
                builder.endArray();
            }
            default -> throw new IllegalArgumentException("Unsupported attribute value type: " + value.getValueCase());
        }
    }

    public record DataPoint(NumberDataPoint dataPoint, Metric metric) {

        long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        List<KeyValue> getAttributes() {
            return dataPoint().getAttributesList();
        }

        Metric.DataCase getDataCase() {
            return metric.getDataCase();
        }

        public long getStartTimestampUnixNano() {
            return dataPoint.getStartTimeUnixNano();
        }

        public String getUnit() {
            return metric().getUnit();
        }

        public String getMetricName() {
            return metric.getName();
        }
    }

    record DataPointGroup(
        Resource resource,
        String resourceSchemaUrl,
        HashValue128 resourceHash,
        InstrumentationScope scope,
        HashValue128 scopeHash,
        List<DataPoint> dataPoints
    ) {}

    public static class MetricsRequest extends ActionRequest {
        private final BytesReference exportMetricsServiceRequest;

        public MetricsRequest(StreamInput in) throws IOException {
            super(in);
            exportMetricsServiceRequest = in.readBytesReference();
        }

        public MetricsRequest(BytesReference exportMetricsServiceRequest) {
            this.exportMetricsServiceRequest = exportMetricsServiceRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class MetricsResponse extends ActionResponse {
        private final MessageLite response;

        public MetricsResponse(MessageLite response) {
            this.response = response;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.write(response.toByteArray());
        }

        public MessageLite getResponse() {
            return response;
        }
    }
}
