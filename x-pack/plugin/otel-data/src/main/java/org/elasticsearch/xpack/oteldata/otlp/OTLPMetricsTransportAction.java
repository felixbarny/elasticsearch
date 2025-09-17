/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) Metrics requests.
 * This action processes the incoming metrics data, groups data points, and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the metrics.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPMetricsTransportAction extends AbstractOtlpTransportAction {

    public static final String NAME = "indices:data/write/otlp/metrics";
    public static final ActionType<OtlpActionResponse> TYPE = new ActionType<>(NAME);

    @Inject
    public OTLPMetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client
    ) {
        super(NAME, transportService, actionFilters, threadPool, client);
    }

    @Override
    protected Context prepareBulkRequest(OtlpActionRequest request, BulkRequestBuilder bulkRequestBuilder) throws IOException {
        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        DataPointGroupingContext context = new DataPointGroupingContext(byteStringAccessor);
        var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.getRequest().streamInput());
        context.groupDataPoints(metricsServiceRequest);
        if (context.totalDataPoints() == 0) {
            return context;
        }
        MetricDocumentBuilder metricDocumentBuilder = new MetricDocumentBuilder(byteStringAccessor);
        context.consume(dataPointGroup -> addIndexRequest(bulkRequestBuilder, metricDocumentBuilder, dataPointGroup));
        return context;
    }

    private void addIndexRequest(
        BulkRequestBuilder bulkRequestBuilder,
        MetricDocumentBuilder metricDocumentBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup
    ) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            var dynamicTemplates = metricDocumentBuilder.buildMetricDocument(xContentBuilder, dataPointGroup);
            bulkRequestBuilder.add(
                new IndexRequest(dataPointGroup.targetIndex().index()).opType(DocWriteRequest.OpType.CREATE)
                    .setRequireDataStream(true)
                    .source(xContentBuilder)
                    .setDynamicTemplates(dynamicTemplates)
            );
        }
    }

}
