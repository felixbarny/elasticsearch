/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import com.google.protobuf.MessageLite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public abstract class AbstractOtlpTransportAction extends HandledTransportAction<OtlpActionRequest, OtlpActionResponse> {

    private static final Logger logger = LogManager.getLogger(AbstractOtlpTransportAction.class);
    private final Client client;

    @Inject
    public AbstractOtlpTransportAction(
        String name,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client
    ) {
        super(name, transportService, actionFilters, OtlpActionRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, OtlpActionRequest request, ActionListener<OtlpActionResponse> listener) {
        Context context = Context.EMPTY;
        try {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            context = prepareBulkRequest(request, bulkRequestBuilder);
            if (bulkRequestBuilder.numberOfActions() == 0) {
                if (context.rejectedDataPoints() == 0) {
                    handleEmptyRequest(listener);
                } else {
                    // all data points were ignored
                    handlePartialSuccess(listener, context);
                }
                return;
            }

            Context finalContext = context;
            bulkRequestBuilder.execute(new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    if (bulkItemResponses.hasFailures() || finalContext.rejectedDataPoints() > 0) {
                        handlePartialSuccess(bulkItemResponses, listener, finalContext);
                    } else {
                        handleSuccess(listener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    handleFailure(listener, e, finalContext);
                }
            });

        } catch (Exception e) {
            logger.error("failed to execute otlp metrics request", e);
            handleFailure(listener, e, context);
        }
    }

    public interface Context {

        Context EMPTY = () -> 0;

        int totalDataPoints();

        default int rejectedDataPoints() {
            return 0;
        }

        default Collection<String> errorMessages() {
            return List.of();
        }
    }

    protected abstract Context prepareBulkRequest(OtlpActionRequest request, BulkRequestBuilder bulkRequestBuilder)
        throws IOException;

    public static void handleSuccess(ActionListener<OtlpActionResponse> listener) {
        listener.onResponse(new OtlpActionResponse(RestStatus.OK, ExportMetricsServiceResponse.newBuilder().build()));
    }

    public static void handleEmptyRequest(ActionListener<OtlpActionResponse> listener) {
        // If the server receives an empty request
        // (a request that does not carry any telemetry data)
        // the server SHOULD respond with success.
        // https://opentelemetry.io/docs/specs/otlp/#full-success-1
        handleSuccess(listener);
    }

    public static void handlePartialSuccess(ActionListener<OtlpActionResponse> listener, Context context) {
        // If the request is only partially accepted
        // (i.e. when the server accepts only parts of the data and rejects the rest),
        // the server MUST respond with HTTP 200 OK.
        // https://opentelemetry.io/docs/specs/otlp/#partial-success-1
        MessageLite response = responseWithRejectedDataPoints(context, context.rejectedDataPoints() + 0);
        listener.onResponse(new OtlpActionResponse(RestStatus.BAD_REQUEST, response));
    }

    public static void handlePartialSuccess(BulkResponse bulkItemResponses, ActionListener<OtlpActionResponse> listener, Context context) {
        // If the request is only partially accepted
        // (i.e. when the server accepts only parts of the data and rejects the rest),
        // the server MUST respond with HTTP 200 OK.
        // https://opentelemetry.io/docs/specs/otlp/#partial-success-1
        RestStatus status = RestStatus.OK;
        int failures = 0;
        for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
            failures += bulkItemResponse.isFailed() ? 1 : 0;
            if (bulkItemResponse.isFailed() && bulkItemResponse.getFailure().getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                // If the server receives more requests than the client is allowed or the server is overloaded,
                // the server SHOULD respond with HTTP 429 Too Many Requests or HTTP 503 Service Unavailable
                // and MAY include “Retry-After” header with a recommended time interval in seconds to wait before retrying.
                // https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling
                status = RestStatus.TOO_MANY_REQUESTS;
            }
        }
        context.errorMessages().add(bulkItemResponses.buildFailureMessage());
        MessageLite response = responseWithRejectedDataPoints(context, context.rejectedDataPoints() + failures);
        listener.onResponse(new OtlpActionResponse(status, response));
    }

    public static void handleFailure(ActionListener<OtlpActionResponse> listener, Exception e, Context context) {
        context.errorMessages().add(e.getMessage());
        // https://opentelemetry.io/docs/specs/otlp/#failures-1
        // If the processing of the request fails,
        // the server MUST respond with appropriate HTTP 4xx or HTTP 5xx status code.
        listener.onResponse(
            new OtlpActionResponse(
                ExceptionsHelper.status(e),
                responseWithRejectedDataPoints(context, context.totalDataPoints())
            )
        );

    }

    private static ExportMetricsPartialSuccess responseWithRejectedDataPoints(Context context, int rejectedDataPoints) {
        return ExportMetricsServiceResponse.newBuilder()
            .getPartialSuccessBuilder()
            .setRejectedDataPoints(rejectedDataPoints)
            .setErrorMessage(String.join("\n", context.errorMessages()))
            .build();
    }

}
