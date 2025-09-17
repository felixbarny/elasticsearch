/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static io.opentelemetry.api.logs.Severity.INFO;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class OTLPLogsIndexingRestIT extends AbstractOTLPIndexingRestIT {

    private SdkLoggerProvider loggerProvider;
    private Logger logger;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        OtlpHttpLogRecordExporter exporter = OtlpHttpLogRecordExporter.builder()
            .setEndpoint(getClusterHosts().getFirst().toURI() + getOtlpEndpoint())
            .addHeader("Authorization", basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray())))
            .build();
        loggerProvider = SdkLoggerProvider.builder()
            .setResource(TEST_RESOURCE)
            .addLogRecordProcessor(BatchLogRecordProcessor.builder(exporter).build())
            .build();
        logger = loggerProvider.get(getClass().getSimpleName());
    }

    @Override
    protected String getOtlpEndpoint()  {
        return "/_otlp/v1/logs";
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (loggerProvider != null) {
            loggerProvider.close();
        }
    }

    public void testLogIndexing() throws Exception {
        int numLogs = 10;
        for (int i = 0; i < numLogs; i++) {
            logger.logRecordBuilder().setBody("Hello world").setSeverity(INFO).setSeverityText("INFO").emit();
        }
        indexLogs();
        var response = client().performRequest(new Request("GET", "logs-generic.otel-default/_search"));
        assertOK(response);
        ObjectPath searchResponse = ObjectPath.createFromResponse(response);
        assertThat(searchResponse.evaluate("hits.total.value"), equalTo(numLogs));
        for (int i = 0; i < numLogs; i++) {
            var source = new ObjectPath(searchResponse.evaluate("hits.hits." + i + "._source"));
            assertThat(source.evaluate("@timestamp"), isA(String.class));
            assertThat(source.evaluate("observed_timestamp"), isA(String.class));
            assertThat(source.evaluate("severity_text"), equalTo("INFO"));
            assertThat(source.evaluate("severity_number"), equalTo(INFO.getSeverityNumber()));
            assertThat(source.evaluate("data_stream.type"), equalTo("logs"));
            assertThat(source.evaluate("data_stream.dataset"), equalTo("generic.otel"));
            assertThat(source.evaluate("data_stream.namespace"), equalTo("default"));
            assertThat(source.evaluate("body.text"), equalTo("Hello world"));
            assertThat(source.evaluate("resource.attributes.service\\.name"), equalTo("elasticsearch"));

        }
    }

    private void indexLogs() throws IOException {
        var result = loggerProvider.forceFlush().join(TEST_REQUEST_TIMEOUT.millis(), MILLISECONDS);
        assertThat(result.isSuccess(), equalTo(true));
        refresh("logs-*.otel-default");
    }

}
