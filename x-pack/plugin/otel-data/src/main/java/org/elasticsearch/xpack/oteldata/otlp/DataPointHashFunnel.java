/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.dynatrace.hash4j.hashing.HashFunnel;
import com.dynatrace.hash4j.hashing.HashSink;

public class DataPointHashFunnel implements HashFunnel<OTLPMetricsTransportAction.DataPoint> {

    private static final DataPointHashFunnel INSTANCE = new DataPointHashFunnel();

    private DataPointHashFunnel() {}

    public static DataPointHashFunnel get() {
        return INSTANCE;
    }

    @Override
    public void put(OTLPMetricsTransportAction.DataPoint dataPoint, HashSink hashSink) {
        hashSink.putLong(dataPoint.getTimestampUnixNano());
        hashSink.putLong(dataPoint.getStartTimestampUnixNano());
        hashSink.putString(dataPoint.getUnit());
        hashSink.putInt(dataPoint.getDataCase().getNumber());
        hashSink.put(dataPoint.getAttributes(), AttributeListHashFunnel.get());
    }
}
