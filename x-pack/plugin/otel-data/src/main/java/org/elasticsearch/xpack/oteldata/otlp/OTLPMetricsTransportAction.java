/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

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

import com.dynatrace.hash4j.hashing.HashStream32;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher128;
import com.dynatrace.hash4j.hashing.Hasher32;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.CheckedConsumer;
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
                        logger.warn("OTLP request completed with failures {}", bulkItemResponses.buildFailureMessage());
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
        DataPointGroupingContext context = new DataPointGroupingContext();
        List<ResourceMetrics> resourceMetricsList = exportMetricsServiceRequest.getResourceMetricsList();
        for (int i = 0; i < resourceMetricsList.size(); i++) {
            ResourceMetrics resourceMetrics = resourceMetricsList.get(i);
            List<KeyValue> resourceAttributes = resourceMetrics.getResource().getAttributesList();
            HashValue128 resourceHash = HASHER_128.hashStream()
                .put(resourceAttributes, AttributeListHashFunnel.get())
                .putString(resourceMetrics.getSchemaUrl())
                .get();
            List<ScopeMetrics> scopeMetricsList = resourceMetrics.getScopeMetricsList();
            for (int j = 0; j < scopeMetricsList.size(); j++) {
                ScopeMetrics scopeMetrics = scopeMetricsList.get(j);
                InstrumentationScope scope = scopeMetrics.getScope();
                List<KeyValue> scopeAttributes = scope.getAttributesList();
                HashValue128 scopeHash = HASHER_128.hashStream()
                    .put(scopeAttributes, AttributeListHashFunnel.get())
                    .putString(scope.getName())
                    .putString(scope.getVersion())
                    .putString(scopeMetrics.getSchemaUrl())
                    .get();
                List<Metric> metricsList = scopeMetrics.getMetricsList();
                for (int k = 0; k < metricsList.size(); k++) {
                    var metric = metricsList.get(k);
                    // TODO: add support for other metric types
                    switch (metric.getDataCase()) {
                        case SUM -> {
                            List<NumberDataPoint> dataPointsList = metric.getSum().getDataPointsList();
                            for (int l = 0; l < dataPointsList.size(); l++) {
                                context.addDataPoint(
                                    resourceHash,
                                    resourceMetrics.getResource(),
                                    resourceMetrics.getSchemaUrl(),
                                    scopeHash,
                                    scope,
                                    scopeMetrics.getSchemaUrl(),
                                    new DataPoint(dataPointsList.get(l), metric)
                                );
                            }
                        }
                        case GAUGE -> {
                            List<NumberDataPoint> dataPointsList = metric.getGauge().getDataPointsList();
                            for (int l = 0; l < dataPointsList.size(); l++) {
                                context.addDataPoint(
                                    resourceHash,
                                    resourceMetrics.getResource(),
                                    resourceMetrics.getSchemaUrl(),
                                    scopeHash,
                                    scope,
                                    scopeMetrics.getSchemaUrl(),
                                    new DataPoint(dataPointsList.get(l), metric)
                                );
                            }
                        }
                        default -> throw new IllegalArgumentException("Unsupported metric type [" + metric.getDataCase() + "]");
                    }
                }
            }
        }
        Set<String> fragmentIds = new HashSet<>();
        context.forEach(dataPointGroup -> createNumberDataPointDoc(fragmentIds, bulkRequestBuilder, dataPointGroup));
    }

    private static class DataPointGroupingContext {
        // <resourceHash, <scopeHash, <dataPointDimensionsHash, List<DataPoint>>>
        private final Map<HashValue128, Map<HashValue128, Map<HashValue128, DataPointGroup>>> dataPoints = new HashMap<>();

        public void addDataPoint(
            HashValue128 resourceHash,
            Resource resource,
            String resourceSchemaUrl,
            HashValue128 scopeHash,
            InstrumentationScope scope,
            String scopeSchemaUrl,
            DataPoint dataPoint
        ) {
            HashValue128 dataPointGroupHash = HASHER_128.hashTo128Bits(dataPoint, DataPointDocumentGroupHashFunnel.get());
            DataPointGroup dataPointGroup = dataPoints.computeIfAbsent(resourceHash, k -> new HashMap<>())
                .computeIfAbsent(scopeHash, k -> new HashMap<>())
                .computeIfAbsent(
                    dataPointGroupHash,
                    k -> new DataPointGroup(
                        resource,
                        resourceSchemaUrl,
                        resourceHash.toString(),
                        scope,
                        scopeSchemaUrl,
                        scopeHash.toString(),
                        dataPointGroupHash.toString(),
                        dataPoint.getAttributes(),
                        dataPoint.getUnit(),
                        new ArrayList<>(),
                        IndexRouting.route(
                            "metrics",
                            dataPoint.getAttributes(),
                            scope.getName(),
                            scope.getAttributesList(),
                            resource.getAttributesList()
                        )
                    )
                );
            dataPointGroup.dataPoints().add(dataPoint);
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
    }

    private void createNumberDataPointDoc(Set<String> fragmentIds, BulkRequestBuilder bulkRequestBuilder, DataPointGroup dataPointGroup)
        throws IOException {
        try (XContentBuilder xContentBuilder = CborXContent.contentBuilder()) {
            List<String> dataPointFragmentIds = buildDataPointDoc(bulkRequestBuilder, fragmentIds, xContentBuilder, dataPointGroup);
            bulkRequestBuilder.add(
                client.prepareIndex("metrics-generic.otel-default")
                    .setCreate(true)
                    .setRequireDataStream(true)
                    .setPipeline(IngestService.NOOP_PIPELINE_NAME)
                    .setSource(xContentBuilder)
                    .setFragmentIds(dataPointFragmentIds)
            );
        }
    }

    private List<String> buildDataPointDoc(
        BulkRequestBuilder requestBuilder,
        Set<String> fragmentIds,
        XContentBuilder builder,
        DataPointGroup dataPointGroup
    ) throws IOException {
        List<String> fragments = new ArrayList<>();
        List<DataPoint> dataPoints = dataPointGroup.dataPoints();
        builder.startObject();
        builder.field("@timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getTimestampUnixNano()));
        if (dataPointGroup.getStartTimestampUnixNano() != 0) {
            builder.field("start_timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getStartTimestampUnixNano()));
        }
        fragments.add(
            addFragmentIfMissing(
                fragmentIds,
                requestBuilder,
                b -> buildResource(dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl(), b),
                dataPointGroup.resourceHash()
            )
        );
        if (dataPointGroup.indexRouting().isDataStream()) {
            fragments.add(
                addFragmentIfMissing(
                    fragmentIds,
                    requestBuilder,
                    b -> buildDataStream(b, dataPointGroup.indexRouting()),
                    dataPointGroup.indexRouting().index()
                )
            );
        }
        fragments.add(
            addFragmentIfMissing(
                fragmentIds,
                requestBuilder,
                b -> buildScope(b, dataPointGroup.scopeSchemaUrl(), dataPointGroup.scope()),
                dataPointGroup.scopeHash()
            )
        );
        String dataPointDimensionsHash = HASHER_128.hashTo128Bits(dataPoints.getFirst(), DataPointDimensionsHashFunnel.get()).toString();
        fragments.add(
            addFragmentIfMissing(
                fragmentIds,
                requestBuilder,
                b -> buildDataPointAttributes(b, dataPointGroup.dataPointAttributes(), dataPointGroup.unit()),
                dataPointDimensionsHash
            )
        );
        HashStream32 metricNamesHash = HASHER_32.hashStream();
        dataPoints.stream().map(DataPoint::getMetricName).forEach(metricNamesHash::putString);
        builder.field("_metric_names_hash", Integer.toHexString(metricNamesHash.getAsInt()));
        builder.startObject("metrics");
        for (DataPoint dataPoint : dataPoints) {
            NumberDataPoint dp = dataPoint.dataPoint();
            switch (dp.getValueCase()) {
                case AS_DOUBLE -> builder.field(dataPoint.getMetricName(), dp.getAsDouble());
                case AS_INT -> builder.field(dataPoint.getMetricName(), dp.getAsInt());
            }
        }
        builder.endObject();
        builder.endObject();
        return fragments;
    }

    private void buildResource(Resource resource, String schemaUrl, XContentBuilder builder) throws IOException {
        builder.startObject("resource");
        addFieldIfNotEmpty(builder, "schema_url", schemaUrl);
        if (resource.getDroppedAttributesCount() > 0) {
            builder.field("dropped_attributes_count", resource.getDroppedAttributesCount());
        }
        builder.startObject("attributes");
        buildAttributes(builder, resource.getAttributesList());
        builder.endObject();
        builder.endObject();
    }

    private void buildScope(XContentBuilder builder, String schemaUrl, InstrumentationScope scope) throws IOException {
        builder.startObject("scope");
        addFieldIfNotEmpty(builder, "schema_url", schemaUrl);
        if (scope.getDroppedAttributesCount() > 0) {
            builder.field("dropped_attributes_count", scope.getDroppedAttributesCount());
        }
        addFieldIfNotEmpty(builder, "name", scope.getName());
        addFieldIfNotEmpty(builder, "version", scope.getVersion());
        builder.startObject("attributes");
        buildAttributes(builder, scope.getAttributesList());
        builder.endObject();
        builder.endObject();
    }

    private static void addFieldIfNotEmpty(XContentBuilder builder, String name, String value) throws IOException {
        if (Strings.isNullOrEmpty(value) == false) {
            builder.field(name, value);
        }
    }

    private void buildDataPointAttributes(XContentBuilder builder, List<KeyValue> attributes, String unit) throws IOException {
        builder.startObject("attributes");
        buildAttributes(builder, attributes);
        builder.endObject();
        builder.field("unit", unit);
    }

    private void buildDataStream(XContentBuilder builder, IndexRouting indexRouting) throws IOException {
        if (indexRouting.isDataStream() == false) {
            return;
        }
        builder.startObject("data_stream");
        builder.field("type", indexRouting.type());
        builder.field("dataset", indexRouting.dataset());
        builder.field("namespace", indexRouting.namespace());
        builder.endObject();
    }

    private void buildAttributes(XContentBuilder builder, List<KeyValue> resourceAttributes) throws IOException {
        for (KeyValue attribute : resourceAttributes) {
            switch (attribute.getKey()) {
                case IndexRouting.ELASTICSEARCH_INDEX, IndexRouting.DATA_STREAM_DATASET, IndexRouting.DATA_STREAM_NAMESPACE -> {
                    // ignore
                }
                default -> {
                    builder.field(attribute.getKey());
                    attributeValue(builder, attribute.getValue());
                }
            }
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

    private String addFragmentIfMissing(
        Set<String> fragmentIds,
        BulkRequestBuilder bulkRequestBuilder,
        CheckedConsumer<XContentBuilder, IOException> sourceBuilder,
        String fragmentId
    ) throws IOException {
        if (fragmentIds.add(fragmentId) == false) {
            return fragmentId;
        }
        try (XContentBuilder fragmentBuilder = CborXContent.contentBuilder()) {
            fragmentBuilder.startObject();
            sourceBuilder.accept(fragmentBuilder);
            fragmentBuilder.endObject();

            bulkRequestBuilder.add(client.prepareFragment("metrics-generic.otel-default").setId(fragmentId).setSource(fragmentBuilder));
        }
        return fragmentId;
    }

    public record DataPoint(NumberDataPoint dataPoint, Metric metric) {

        long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        List<KeyValue> getAttributes() {
            return dataPoint().getAttributesList();
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
        String resourceHash,
        InstrumentationScope scope,
        String scopeSchemaUrl,
        String scopeHash,
        String dataPointGroupHash,
        List<KeyValue> dataPointAttributes,
        String unit,
        List<DataPoint> dataPoints,
        IndexRouting indexRouting
    ) {

        public long getTimestampUnixNano() {
            return dataPoints.getFirst().getTimestampUnixNano();
        }

        public long getStartTimestampUnixNano() {
            return dataPoints.getFirst().getStartTimestampUnixNano();
        }
    }

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
