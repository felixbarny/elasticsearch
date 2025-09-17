/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.LogDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) logs requests.
 * This action processes the incoming logs data and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the logs.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPLogsTransportAction extends AbstractOtlpTransportAction {

    public static final String NAME = "indices:data/write/otlp/logs";
    public static final ActionType<OtlpActionResponse> TYPE = new ActionType<>(NAME);

    public static final String TYPE_LOGS = "logs";

    @Inject
    public OTLPLogsTransportAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool, Client client) {
        super(NAME, transportService, actionFilters, threadPool, client);
    }

    @Override
    protected Context prepareBulkRequest(OtlpActionRequest request, BulkRequestBuilder bulkRequestBuilder) throws IOException {
        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        var logsServiceRequest = ExportLogsServiceRequest.parseFrom(request.getRequest().streamInput());
        LogDocumentBuilder logDocumentBuilder = new LogDocumentBuilder(byteStringAccessor);
        List<ResourceLogs> resourceLogsList = logsServiceRequest.getResourceLogsList();
        for (int i = 0, resourceLogsListSize = resourceLogsList.size(); i < resourceLogsListSize; i++) {
            ResourceLogs resourceLogs = resourceLogsList.get(i);
            Resource resource = resourceLogs.getResource();
            List<ScopeLogs> scopeLogsList = resourceLogs.getScopeLogsList();
            for (int j = 0, scopeLogsListSize = scopeLogsList.size(); j < scopeLogsListSize; j++) {
                ScopeLogs scopeLogs = scopeLogsList.get(j);
                InstrumentationScope scope = scopeLogs.getScope();
                List<LogRecord> logRecordsList = scopeLogs.getLogRecordsList();
                for (int k = 0, logRecordsListSize = logRecordsList.size(); k < logRecordsListSize; k++) {
                    LogRecord logRecord = logRecordsList.get(k);
                    TargetIndex index = TargetIndex.evaluate(
                        TYPE_LOGS,
                        logRecord.getAttributesList(),
                        null,
                        scope.getAttributesList(),
                        resource.getAttributesList()
                    );
                    try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
                        logDocumentBuilder.buildLogDocument(
                            xContentBuilder,
                            resource,
                            resourceLogs.getSchemaUrlBytes(),
                            scope,
                            scopeLogs.getSchemaUrlBytes(),
                            index,
                            logRecord
                        );
                        bulkRequestBuilder.add(
                            new IndexRequest(index.index()).opType(DocWriteRequest.OpType.CREATE)
                                .setRequireDataStream(true)
                                .source(xContentBuilder)
                        );
                    }
                }
            }
        }
        return new LogsContext(bulkRequestBuilder.numberOfActions());
    }

    private static class LogsContext implements Context {
        private final int totalDataPoints;

        private LogsContext(int totalDataPoints) {
            this.totalDataPoints = totalDataPoints;
        }

        @Override
        public int totalDataPoints() {
            return totalDataPoints;
        }
    }

}
