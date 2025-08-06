/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xpack.oteldata.otlp.tsid.DataPointGroupTsidFunnel;

import java.io.IOException;
import java.util.Arrays;

public class OTLPMetricsTransportAction extends HandledTransportAction<
    OTLPMetricsTransportAction.MetricsRequest,
    OTLPMetricsTransportAction.MetricsResponse> {

    public static final String NAME = "indices:data/write/metrics";
    public static final ActionType<MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(OTLPMetricsTransportAction.class);
    private final Client client;
    private final MetricDocumentBuilder metricDocumentBuilder = new MetricDocumentBuilder();

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
            DataPointGroupingContext context = new DataPointGroupingContext();
            context.groupDataPoints(metricsServiceRequest);
            context.forEach(dataPointGroup -> addIndexRequest(bulkRequestBuilder, dataPointGroup));

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

    private void addIndexRequest(BulkRequestBuilder bulkRequestBuilder, DataPointGroupingContext.DataPointGroup dataPointGroup)
        throws IOException {
        try (XContentBuilder xContentBuilder = CborXContent.contentBuilder()) {
            var dynamicTemplates = metricDocumentBuilder.buildMetricDocument(xContentBuilder, dataPointGroup);
            bulkRequestBuilder.add(
                client.prepareIndex(dataPointGroup.targetIndex().index())
                    .setCreate(true)
                    .setRequireDataStream(true)
                    .setSource(xContentBuilder)
                    .setTsid(DataPointGroupTsidFunnel.forDataPointGroup(dataPointGroup).buildTsid())
                    .setDynamicTemplates(dynamicTemplates)
            );
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
