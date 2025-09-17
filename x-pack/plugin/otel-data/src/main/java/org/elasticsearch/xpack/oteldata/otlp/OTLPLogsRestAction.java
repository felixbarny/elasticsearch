/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class OTLPLogsRestAction extends AbstractOtlpRestAction {
    public OTLPLogsRestAction() {
        super(OTLPLogsTransportAction.TYPE, ExportMetricsServiceResponse.newBuilder().build());
    }

    @Override
    public String getName() {
        return "otlp_logs_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_otlp/v1/logs"));
    }

}
