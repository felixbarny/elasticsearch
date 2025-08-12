/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;

import java.util.List;

public class ScopeTsidFunnel implements TsidFunnel<ScopeMetrics> {

    private static final ScopeTsidFunnel INSTANCE = new ScopeTsidFunnel();

    public static TsidBuilder forScope(ScopeMetrics scopeMetrics) {
        TsidBuilder tsidBuilder = new TsidBuilder(scopeMetrics.getScope().getAttributesCount() + 3);
        INSTANCE.add(scopeMetrics, tsidBuilder);
        return tsidBuilder;
    }

    @Override
    public void add(ScopeMetrics scopeMetrics, TsidBuilder tsidBuilder) {
        List<KeyValue> resourceAttributes = scopeMetrics.getScope().getAttributesList();
        tsidBuilder.addStringDimension("name", scopeMetrics.getScope().getNameBytes().toByteArray());
        tsidBuilder.addStringDimension("schema_url", scopeMetrics.getSchemaUrlBytes().toByteArray());
        tsidBuilder.add(resourceAttributes, AttributeListTsidFunnel.get("scope.attributes."));
        tsidBuilder.addStringDimension("version", scopeMetrics.getScope().getVersionBytes().toByteArray());
    }
}
