/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.DataPointGroupingContext;

class DataPointDimensionsTsidFunnel implements TsidFunnel<DataPointGroupingContext.DataPoint> {

    private static final DataPointDimensionsTsidFunnel INSTANCE = new DataPointDimensionsTsidFunnel();

    private DataPointDimensionsTsidFunnel() {}

    static DataPointDimensionsTsidFunnel get() {
        return INSTANCE;
    }

    @Override
    public void add(DataPointGroupingContext.DataPoint dataPoint, TsidBuilder tsidBuilder) {
        tsidBuilder.add(dataPoint.getAttributes(), AttributeListTsidFunnel.get("attributes."));
        tsidBuilder.addStringDimension("unit", dataPoint.getUnit());
    }
}
