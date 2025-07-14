/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.action.otlp;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.countedkeyword.CountedKeywordMapperPlugin;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.stack.StackPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;
import org.elasticsearch.xpack.wildcard.Wildcard;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class OTLPMetricsIndexingIT extends ESSingleNodeTestCase {

    private static final Resource TEST_RESOURCE = Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "elasticsearch"));
    private static final InstrumentationScopeInfo TEST_SCOPE = InstrumentationScopeInfo.create("io.opentelemetry.example.metrics");
    private OtlpHttpMetricExporter exporter;
    private SdkMeterProvider meterProvider;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(
            DataStreamsPlugin.class,
            InternalSettingsPlugin.class,
            OTelPlugin.class,
            StackPlugin.class,
            EsqlPlugin.class,
            VersionFieldPlugin.class,
            CountedKeywordMapperPlugin.class,
            ConstantKeywordMapperPlugin.class,
            MapperExtrasPlugin.class,
            Wildcard.class,
            IndexLifecycle.class,
            IngestCommonPlugin.class,
            XPackPlugin.class,
            PainlessPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        // This essentially disables the automatic updates to end_time settings of a data stream's latest backing index.
        newSettings.put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "10m");
        return newSettings.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        exporter = OtlpHttpMetricExporter.builder().setEndpoint("http://localhost:" + getHttpPort() + "/_otlp/v1/metrics").build();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setExecutor(Executors.newScheduledThreadPool(0))
                    .setInterval(Duration.ofNanos(Long.MAX_VALUE))
                    .build()
            )
            .build();
        assertBusy(() -> {
            GetComposableIndexTemplateAction.Request getReq = new GetComposableIndexTemplateAction.Request(TEST_REQUEST_TIMEOUT, "*");
            var templates = client().execute(GetComposableIndexTemplateAction.INSTANCE, getReq).actionGet().indexTemplates();
            assertThat(templates, not(anEmptyMap()));
        });
    }

    private int getHttpPort() {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        assertEquals(1, nodesInfoResponse.getNodes().size());
        NodeInfo node = nodesInfoResponse.getNodes().getFirst();
        assertNotNull(node.getInfo(HttpInfo.class));
        TransportAddress publishAddress = node.getInfo(HttpInfo.class).address().publishAddress();
        InetSocketAddress address = publishAddress.address();
        return address.getPort();
    }

    @Override
    public void tearDown() throws Exception {
        meterProvider.close();
        super.tearDown();
    }

    @Test
    public void testIngestMetricViaMeterProvider() throws Exception {
        Meter sampleMeter = meterProvider.get("io.opentelemetry.example.metrics");

        sampleMeter.gaugeBuilder("jvm.memory.total")
            .setDescription("Reports JVM memory usage.")
            .setUnit("By")
            .buildWithCallback(result -> result.record(Runtime.getRuntime().totalMemory(), Attributes.empty()));

        var result = meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
        assertThat(result.isSuccess(), is(true));

        admin().indices().prepareRefresh().execute().actionGet();
        String[] indices = admin().indices()
            .prepareGetIndex(TimeValue.timeValueSeconds(1))
            .setIndices("metrics-generic.otel-default")
            .get()
            .indices();
        assertThat(indices, not(emptyArray()));

        try (EsqlQueryResponse resp = query("""
            TS metrics-generic.otel-default
             | STATS avg(jvm.memory.total)
            """)) {
            double avgJvmMemoryTotal = (double) resp.column(0).next();
            assertThat(avgJvmMemoryTotal, greaterThan(0.0));
        }
    }

    @Test
    public void testIngestMetricDataViaMetricExporter() throws Exception {
        MetricData jvmMemoryMetricData = getDoubleGauge("jvm.memory.total", Runtime.getRuntime().totalMemory(), Attributes.empty(), "By", Clock.getDefault().now());

        export(List.of(jvmMemoryMetricData));
        String[] indices = admin().indices()
            .prepareGetIndex(TimeValue.timeValueSeconds(1))
            .setIndices("metrics-generic.otel-default")
            .get()
            .indices();
        assertThat(indices, not(emptyArray()));

        try (EsqlQueryResponse resp = query("""
            TS metrics-generic.otel-default
             | STATS avg(jvm.memory.total)
            """)) {
            double avgJvmMemoryTotal = (double) resp.column(0).next();
            assertThat(avgJvmMemoryTotal, greaterThan(0.0));
        }
    }

    @Test
    public void testGroupingSameGroup() throws Exception {
        long now = Clock.getDefault().now();
        MetricData metric1 = getDoubleGauge("metric1", 42, Attributes.empty(), "By", now);
        MetricData metric2 = getDoubleGauge("metric2", 42, Attributes.empty(), "By", now);

        export(List.of(metric1, metric2));

        SearchResponse resp = client().prepareSearch("metrics-generic.otel-default").get();
        assertThat(resp.getHits().getHits(), arrayWithSize(1));
        Map<String, Object> sourceMap = resp.getHits().getAt(0).getSourceAsMap();
        assertThat(sourceMap.get("metrics"), equalTo(Map.of(
            "metric1", 42.0,
            "metric2", 42.0
        )));
        assertThat(sourceMap.get("resource"), equalTo(Map.of("attributes", Map.of("service.name", "elasticsearch"))));
    }

    @Test
    public void testGroupingDifferentGroup() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                getDoubleGauge("metric1", 42, Attributes.empty(), "By", now),
                getDoubleGauge("metric1", 42, Attributes.empty(), "By", now + TimeUnit.MILLISECONDS.toNanos(1)),
                getDoubleGauge("metric1", 42, Attributes.empty(), "", now),
                getDoubleGauge("metric1", 42, Attributes.of(AttributeKey.stringKey("foo"), "bar"), "By", now)
            )
        );

        SearchResponse resp = client().prepareSearch("metrics-generic.otel-default").get();
        assertThat(resp.getHits().getHits(), arrayWithSize(4));
    }

    private void export(List<MetricData> metrics) {
        var result = exporter.export(metrics).join(10, TimeUnit.SECONDS);
        assertThat(result.isSuccess(), is(true));
        admin().indices().prepareRefresh().execute().actionGet();
    }

    private static MetricData getDoubleGauge(String name, double value, Attributes attributes, String unit, long timeEpochNanos) {
        return ImmutableMetricData.createDoubleGauge(
            TEST_RESOURCE,
            TEST_SCOPE,
            name,
            "Your description could be here.",
            unit,
            ImmutableGaugeData.create(
                List.of(ImmutableDoublePointData.create(timeEpochNanos, timeEpochNanos, attributes, value))
            )
        );
    }

    protected EsqlQueryResponse query(String esql) {
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(client()).query(esql).execute().actionGet();
    }
}
