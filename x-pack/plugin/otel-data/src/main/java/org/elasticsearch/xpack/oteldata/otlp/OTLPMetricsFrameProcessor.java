/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.metrics.v1.ResourceMetrics;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.Map;

final class OTLPMetricsFrameProcessor implements OtlpProtobufFrameProcessor {
    private final BufferedByteStringAccessor byteStringAccessor;
    private final DataPointGroupingContext dataPointGroupingContext;
    private final MappingHints mappingHints;

    OTLPMetricsFrameProcessor(MappingHints mappingHints, Client client) {
        this.byteStringAccessor = new BufferedByteStringAccessor();
        this.dataPointGroupingContext = new DataPointGroupingContext(byteStringAccessor, client.prepareBulk());
        this.mappingHints = mappingHints;
    }

    @Override
    public void onFrame(BytesReference frame) throws IOException {
        dataPointGroupingContext.groupResourceMetrics(ResourceMetrics.parseFrom(frame.streamInput()));
    }

    @Override
    public AbstractOTLPTransportAction.ProcessingContext onComplete() {
        try {
            if (dataPointGroupingContext.totalDataPoints() != 0) {
                MetricDocumentBuilder metricDocumentBuilder = new MetricDocumentBuilder(byteStringAccessor, mappingHints);
                dataPointGroupingContext.consume(
                    dataPointGroup -> addIndexRequest(
                        dataPointGroupingContext.getBulkRequestBuilder(),
                        metricDocumentBuilder,
                        dataPointGroup
                    )
                );
            }
        } catch (Exception e) {
            dataPointGroupingContext.onFailure(e);
        }
        return dataPointGroupingContext;
    }

    @Override
    public AbstractOTLPTransportAction.ProcessingContext onFailure(Exception failure) {
        dataPointGroupingContext.onFailure(failure);
        return dataPointGroupingContext;
    }

    @Override
    public int totalDataPoints() {
        return dataPointGroupingContext.totalDataPoints();
    }

    private static void addIndexRequest(
        BulkRequestBuilder bulkRequestBuilder,
        MetricDocumentBuilder metricDocumentBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup
    ) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            var dynamicTemplates = Maps.<String, String>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            var dynamicTemplateParams = Maps.<String, Map<String, String>>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            BytesRef tsid = metricDocumentBuilder.buildMetricDocument(
                xContentBuilder,
                dataPointGroup,
                dynamicTemplates,
                dynamicTemplateParams
            );
            bulkRequestBuilder.add(
                new IndexRequest(dataPointGroup.targetIndex().index()).opType(DocWriteRequest.OpType.CREATE)
                    .setRequireDataStream(true)
                    .source(xContentBuilder)
                    .tsid(tsid)
                    .setIncludeSourceOnError(false)
                    .setDynamicTemplates(dynamicTemplates)
                    .setDynamicTemplateParams(dynamicTemplateParams)
            );
        }
    }
}
