/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.Metric;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.util.List;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.mockito.Mockito.mock;

public class OTLPMetricsTransportActionTests extends AbstractOTLPTransportActionTests {

    @Override
    protected AbstractOTLPTransportAction createAction() {
        return new OTLPMetricsTransportAction(mock(TransportService.class), mock(ActionFilters.class), mock(ThreadPool.class), client);
    }

    @Override
    protected BytesReference createRequestWithData() {
        return createMetricsRequest(createMetric());
    }

    @Override
    protected BytesReference createEmptyRequest() {
        return createMetricsRequest();
    }

    @Override
    protected OtlpProtobufFrameProcessor getFrameProcessor() {
        return new OTLPMetricsFrameProcessor(MappingHints.DEFAULT_TDIGEST, client);
    }

    @Override
    protected boolean parseHasPartialSuccess(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).hasPartialSuccess();
    }

    @Override
    protected long parseRejectedCount(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getRejectedDataPoints();
    }

    @Override
    protected String parseErrorMessage(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getErrorMessage();
    }

    @Override
    protected String dataStreamType() {
        return "metrics";
    }

    // --- helpers ---

    private BytesReference createMetricsRequest(Metric... metrics) {
        return new BytesArray(
            OtlpUtils.createResourceMetrics(
                List.of(keyValue("service.name", "test-service")),
                List.of(OtlpUtils.createScopeMetrics("test", "1.0.0", List.of(metrics)))
            ).toByteArray()
        );
    }

    private static Metric createMetric() {
        return OtlpUtils.createGaugeMetric("test.metric", "", List.of(OtlpUtils.createDoubleDataPoint(0)));
    }
}
