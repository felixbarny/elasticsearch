/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;

import java.util.List;

public class ResourceTsidFunnel implements TsidFunnel<ResourceMetrics> {

    private static final ResourceTsidFunnel INSTANCE = new ResourceTsidFunnel();

    public static TsidBuilder forResource(ResourceMetrics resourceMetrics) {
        TsidBuilder tsidBuilder = new TsidBuilder(resourceMetrics.getResource().getAttributesCount() + 1);
        INSTANCE.add(resourceMetrics, tsidBuilder);
        return tsidBuilder;
    }

    @Override
    public void add(ResourceMetrics resourceMetrics, TsidBuilder tsidBuilder) {
        List<KeyValue> resourceAttributes = resourceMetrics.getResource().getAttributesList();
        tsidBuilder.add(resourceAttributes, AttributeListTsidFunnel.get("resource.attributes."));
        tsidBuilder.addStringDimension("schema_url", resourceMetrics.getSchemaUrlBytes().toByteArray());
    }
}
