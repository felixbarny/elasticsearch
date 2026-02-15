/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class OTLPMetricsRestAction extends AbstractOTLPRestAction {

    private volatile MappingHints defaultMappingHints;

    public OTLPMetricsRestAction(ClusterSettings clusterSettings) {
        super(OTLPMetricsTransportAction.TYPE, ExportMetricsServiceResponse.newBuilder().build());
        defaultMappingHints = MappingHints.fromSettings(clusterSettings.get(OTelPlugin.USE_EXPONENTIAL_HISTOGRAM_FIELD_TYPE));
        clusterSettings.addSettingsUpdateConsumer(OTelPlugin.USE_EXPONENTIAL_HISTOGRAM_FIELD_TYPE, histogramFieldTypeSetting -> {
            defaultMappingHints = MappingHints.fromSettings(histogramFieldTypeSetting);
        });
    }

    @Override
    public String getName() {
        return "otlp_metrics_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_otlp/v1/metrics"));
    }

    @Override
    protected OtlpProtobufFrameProcessor createFrameProcessor(NodeClient client) {
        return new OTLPMetricsFrameProcessor(defaultMappingHints, client);
    }

    @Override
    protected int protoFramedFieldNumber() {
        return ExportMetricsServiceRequest.RESOURCE_METRICS_FIELD_NUMBER;
    }

}
