/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) Metrics requests.
 * This action processes the incoming metrics data, groups data points, and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the metrics.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPMetricsTransportAction extends AbstractOTLPTransportAction {

    public static final String NAME = "indices:data/write/otlp/metrics";
    public static final ActionType<OTLPActionResponse> TYPE = new ActionType<>(NAME);

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
    protected ExportMetricsServiceResponse responseWithRejectedDataPoints(int rejectedDataPoints, String message) {
        ExportMetricsPartialSuccess partialSuccess = ExportMetricsPartialSuccess.newBuilder()
            .setRejectedDataPoints(rejectedDataPoints)
            .setErrorMessage(message)
            .build();
        return ExportMetricsServiceResponse.newBuilder().setPartialSuccess(partialSuccess).build();
    }
}
