/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.logs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.MapUtils;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Predicate.not;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestLogsAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestLogsAction.class);

    @Override
    public String getName() {
        return "logs_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_logs"),
            new Route(POST, "/_logs/{data_stream.dataset}"),
            new Route(POST, "/_logs/{data_stream.dataset}/{data_stream.namespace}")
        );
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> globalMetadata = new HashMap<>();
        Map<String, String> params = request.params();
        params.entrySet()
            .stream()
            .filter(not(e -> e.getKey().startsWith("_")))
            .forEach(e -> addPath(globalMetadata, e.getKey(), request.param(e.getKey())));

        Predicate<String> firstLine = s -> false;
        Predicate<String> lastLine = s -> true;
        boolean negate = request.paramAsBoolean("_multiline.negate", false);
        if (params.containsKey("_multiline.first_line_pattern")) {
            Pattern firstLinePattern = Pattern.compile(request.param("_multiline.first_line_pattern"));
            firstLine = line -> firstLinePattern.matcher(line).find();
            if (negate) {
                firstLine = firstLine.negate();
            }
            lastLine = line -> false;
        }
        if (params.containsKey("_multiline.last_line_pattern")) {
            Pattern lastLinePattern = Pattern.compile(request.param("_multiline.last_line_pattern"));
            lastLine = line -> lastLinePattern.matcher(line).find();
            if (negate) {
                lastLine = lastLine.negate();
            }
        }

        List<IndexRequest> indexRequests = new ArrayList<>();
        Charset charset = Optional.ofNullable(request.getParsedContentType())
            .map(ct -> ct.getParameters().get("charset"))
            .map(Charset::forName)
            .orElse(UTF_8);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(request.content().streamInput(), charset))) {
            Map<String, Object> localMetadata = new HashMap<>();
            int i = 0;
            StringBuilder logEventBuilder = new StringBuilder();
            boolean jsonEvent = false;
            for (String line = reader.readLine(); line != null; line = reader.readLine(), i++) {
                if (logEventBuilder.isEmpty() || line.startsWith("{") || (jsonEvent == false && firstLine.test(line))) {
                    jsonEvent = line.startsWith("{");
                    if (logEventBuilder.length() > 0) {
                        processRawLogEvent(indexRequests, globalMetadata, localMetadata, i, logEventBuilder.toString(), jsonEvent);
                        logEventBuilder.setLength(0);
                    }
                    logEventBuilder.append(line);
                } else {
                    logEventBuilder.append("\n").append(line);
                }
                if ((jsonEvent && line.endsWith("}")) || (jsonEvent == false && lastLine.test(line))) {
                    processRawLogEvent(indexRequests, globalMetadata, localMetadata, i, logEventBuilder.toString(), jsonEvent);
                    logEventBuilder.setLength(0);
                }
            }
            if (logEventBuilder.length() > 0) {
                processRawLogEvent(indexRequests, globalMetadata, localMetadata, i, logEventBuilder.toString(), jsonEvent);
                logEventBuilder.setLength(0);
            }
        }

        return channel -> {
            client.bulk(Requests.bulkRequest().add(indexRequests), new RestActionListener<>(channel) {
                @Override
                protected void processResponse(BulkResponse bulkItemResponses) throws Exception {
                    if (bulkItemResponses.hasFailures() == false) {
                        sendResponse(channel, RestStatus.ACCEPTED, b -> {});
                        return;
                    }
                    BulkRequest retryBulk = Requests.bulkRequest();
                    Arrays.stream(bulkItemResponses.getItems()).filter(BulkItemResponse::isFailed).forEach(failedRequest -> {
                        IndexRequest originalRequest = indexRequests.get(failedRequest.getItemId());
                        Map<String, Object> doc = originalRequest.sourceAsMap();
                        BulkItemResponse.Failure failure = failedRequest.getFailure();
                        if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                            // looks like a transient error; re-try as-is
                            retryBulk.add(Requests.indexRequest(originalRequest.index()).opType(DocWriteRequest.OpType.CREATE).source(doc));
                        } else {
                            // looks like a persistent error (such as a mapping issue);
                            // re-try in fallback data stream which has lenient mappings
                            Exception cause = failure.getCause();
                            addPath(doc, "ingest.error.type", ElasticsearchException.getExceptionName(cause));
                            addPath(doc, "ingest.error.message", cause.getMessage());
                            @SuppressWarnings("unchecked")
                            Map<String, String> dataStream = (Map<String, String>) doc.get("data_stream");
                            addPath(doc, "ingest.error.data_stream", new HashMap<>(dataStream));
                            dataStream.put("type", "logs");
                            dataStream.put("dataset", "generic");
                            retryBulk.add(
                                Requests.indexRequest(routeToDataStream(dataStream)).opType(DocWriteRequest.OpType.CREATE).source(doc)
                            );
                        }
                    });
                    client.bulk(retryBulk, new RestActionListener<>(channel) {
                        @Override
                        protected void processResponse(BulkResponse bulkItemResponses) throws Exception {
                            if (bulkItemResponses.hasFailures() == false) {
                                sendResponse(channel, RestStatus.ACCEPTED, b -> {});
                            } else {
                                sendResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, b -> {});
                                logger.error(
                                    "Failed to ingest logs: re-try batch has failures. First failure: {}",
                                    Arrays.stream(bulkItemResponses.getItems())
                                        .filter(BulkItemResponse::isFailed)
                                        .findFirst()
                                        .map(BulkItemResponse::getFailureMessage)
                                        .orElse(null)
                                );
                            }
                        }
                    });
                }
            });
        };
    }

    private void processRawLogEvent(
        List<IndexRequest> indexRequests,
        Map<String, Object> globalMetadata,
        Map<String, Object> localMetadata,
        int i,
        String logEvent,
        boolean jsonEvent
    ) {
        Map<String, Object> event;
        if (jsonEvent) {
            try {
                event = parseJson(logEvent);
                expandDots(event);
            } catch (Exception e) {
                event = new HashMap<>();
                addPath(event, "event.original", logEvent);
                addPath(event, "ingest.error.type", ElasticsearchException.getExceptionName(e));
                addPath(event, "ingest.error.message", e.getMessage());
            }
        } else {
            event = Map.of("message", logEvent);
        }

        if (event.size() == 1 && event.containsKey("_metadata")) {
            Map<String, Object> metadata = getMetadata(event);
            expandDots(metadata);
            if (i == 0) {
                MapUtils.recursiveMerge(globalMetadata, metadata);
            } else {
                localMetadata.clear();
                localMetadata.putAll(metadata);
            }
        } else {
            addEventToBulk(indexRequests, globalMetadata, localMetadata, event);
        }
    }

    private void addEventToBulk(
        List<IndexRequest> indexRequests,
        Map<String, Object> globalMetadata,
        Map<String, Object> localMetadata,
        Map<String, Object> event
    ) {
        HashMap<String, Object> doc = new HashMap<>(globalMetadata);
        MapUtils.recursiveMerge(doc, localMetadata);
        MapUtils.recursiveMerge(doc, event);
        if (doc.containsKey("@timestamp") == false) {
            String now = Instant.now().toString();
            doc.put("@timestamp", now);
        }
        // routing based on data_stream.* fields
        // this part will be handled by document based routing in the future
        // for example, by a routing pipeline that is attached to the logs-router-default data stream
        doc.putIfAbsent("data_stream", new HashMap<>());
        @SuppressWarnings("unchecked")
        Map<String, String> dataStream = (Map<String, String>) doc.get("data_stream");
        dataStream.put("type", "logs");
        dataStream.putIfAbsent("dataset", "generic");
        dataStream.putIfAbsent("namespace", "default");
        indexRequests.add(Requests.indexRequest(routeToDataStream(dataStream)).opType(DocWriteRequest.OpType.CREATE).source(doc));
    }

    private String routeToDataStream(Map<String, String> dataStream) {
        // TODO validate or sanitize dataset and namespace
        return "logs-" + dataStream.getOrDefault("dataset", "generic") + "-" + dataStream.getOrDefault("namespace", "default");
    }

    public void sendResponse(RestChannel channel, RestStatus status, Consumer<XContentBuilder> builderConsumer) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builderConsumer.accept(builder);
            channel.sendResponse(new RestResponse(status, builder));
        }
    }

    private Map<String, Object> parseJson(String json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.allowDuplicateKeys(true);
            return parser.map();
        }
    }

    private Map<String, Object> getMetadata(Map<String, ?> event) {
        Object metadata = event.get("_metadata");
        if (metadata instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metadataMap = (Map<String, Object>) metadata;
            return metadataMap;
        }
        return Map.of();
    }

    public static void expandDots(Map<String, Object> doc) {
        for (String key : new ArrayList<>(doc.keySet())) {
            if (key.contains(".")) {
                Object value = doc.remove(key);
                addPath(doc, key, value);
            }
        }
    }

    private static void addPath(Map<String, Object> doc, String path, Object value) {
        Map<String, Object> parent = doc;
        String[] pathElements = path.split("\\.");
        for (int i = 0; i < pathElements.length - 1; i++) {
            String pathElement = pathElements[i];
            if (parent.containsKey(pathElement) == false) {
                parent.put(pathElement, new HashMap<>());
            }
            Object potentialParent = parent.get(pathElement);
            if (potentialParent instanceof Map) {
                // as this is a json object, if it's a map, it's guaranteed to be a Map<String, Object>
                // that's because there can't be non-string keys in json objects
                @SuppressWarnings("unchecked")
                Map<String, Object> mapParent = (Map<String, Object>) potentialParent;
                parent = mapParent;
            } else {
                // conflict, put the dotted key back in
                doc.put(path, value);
                return;
            }
        }
        parent.put(pathElements[pathElements.length - 1], value);
    }
}
