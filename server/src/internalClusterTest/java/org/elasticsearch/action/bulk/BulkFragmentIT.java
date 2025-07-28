/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.fragment.FragmentRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numClientNodes = 1, minNumDataNodes = 2)
public class BulkFragmentIT extends ESIntegTestCase {

    public void testBulkFragmentReplica() throws Exception {
        createIndex(
            "index",
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put("index.mapping.source.mode", randomFrom("synthetic", "stored"))
                .build()
        );

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkRequest.add(new FragmentRequest().index("index").id("fragment1").source(Map.of("foo", "bar")));
        bulkRequest.add(new IndexRequest().index("index").id("1").source(Map.of("baz", "qux")).fragmentIds(List.of("fragment1")));

        BulkResponse bulkResponse = internalCluster().coordOnlyNodeClient().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(false));

        // Wait for the index to be fully available with all replicas
        ensureGreen("index");

        Set<String> nodes = internalCluster().nodesInclude("index");
        assertThat(nodes.size(), greaterThanOrEqualTo(2)); // minimum of 1 replica
        for (String node : nodes) {
            assertResponse(prepareSearch("index").setPreference("_only_nodes:" + node), response -> {
                assertHitCount(response, 1L);
                assertThat(response.getHits().getAt(0).getSourceAsMap(), equalTo(Map.of("foo", "bar", "baz", "qux")));
            });
        }
    }

    public void testFragmentNotReferenced() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkRequest.add(new FragmentRequest().index("index").id("fragment1").source(Map.of("foo", "bar")));
        bulkRequest.add(new IndexRequest().index("index").id("1").source(Map.of("baz", "qux")));

        BulkResponse bulkResponse = internalCluster().coordOnlyNodeClient().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(true));

        // Verify that the fragment operation failed because it wasn't used
        BulkItemResponse fragmentResponse = bulkResponse.getItems()[0];
        assertThat(fragmentResponse.isFailed(), is(true));
        assertThat(fragmentResponse.getFailureMessage(), containsString("fragment [fragment1] was not used"));

        // Verify that the index operation succeeded
        BulkItemResponse indexResponse = bulkResponse.getItems()[1];
        assertThat(indexResponse.isFailed(), is(false));
    }

    public void testIllegalForwardReference() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // Reference a fragment before it's declared
        bulkRequest.add(new IndexRequest().index("index").id("1").source(Map.of("baz", "qux")).fragmentIds(List.of("fragment1")));
        // Add the fragment after (which is illegal)
        bulkRequest.add(new FragmentRequest().index("index").id("fragment1").source(Map.of("foo", "bar")));

        BulkResponse bulkResponse = internalCluster().coordOnlyNodeClient().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(true));
        BulkItemResponse failedItem = bulkResponse.getItems()[0];
        assertThat(failedItem.isFailed(), is(true));
        assertThat(
            failedItem.getFailureMessage(),
            containsString(
                "Fragment with id [fragment1] not found in the current context. "
                    + "Fragments need to be defined before other requests that reference them."
            )
        );
    }

    public void testInvalidFragmentSource() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        // Add a fragment with invalid JSON source that will fail parsing
        bulkRequest.add(new FragmentRequest().index("index").id("fragment1").source(new BytesArray("{invalid_json"), XContentType.JSON));
        bulkRequest.add(new IndexRequest().index("index").id("1").source(Map.of("baz", "qux")).fragmentIds(List.of("fragment1")));

        BulkResponse bulkResponse = internalCluster().coordOnlyNodeClient().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(true));
        for (BulkItemResponse failedItem : bulkResponse.getItems()) {
            assertThat(failedItem.isFailed(), is(true));
            assertThat(ExceptionsHelper.unwrapCause(failedItem.getFailure().getCause()), isA(DocumentParsingException.class));
        }
    }

    public void testCrossIndexFragmentReference() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // Add a fragment to index1
        bulkRequest.add(new FragmentRequest().index("index1").id("fragment1").source(Map.of("foo", "bar")));
        // Try to reference it from index2 (which should fail)
        bulkRequest.add(new IndexRequest().index("index2").id("1").source(Map.of("baz", "qux")).fragmentIds(List.of("fragment1")));

        BulkResponse bulkResponse = internalCluster().coordOnlyNodeClient().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(true));
        BulkItemResponse failedItem = bulkResponse.getItems()[1]; // The second operation should fail
        assertThat(failedItem.isFailed(), is(true));
        assertThat(
            failedItem.getFailureMessage(),
            containsString("Fragment with id [fragment1] has a different index [index1] than the current index [index2]")
        );
    }

    public void testMultipleFragmentsPerDocument() throws Exception {
        createIndex(
            "index",
            Settings.builder()
                .put("index.mapping.source.mode", randomFrom("synthetic", "stored"))
                .build()
        );

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // Add multiple fragments
        bulkRequest.add(new FragmentRequest().index("index").id("fragment1").source(Map.of("field1", "value1")));
        bulkRequest.add(new FragmentRequest().index("index").id("fragment2").source(Map.of("field2", "value2")));
        bulkRequest.add(new FragmentRequest().index("index").id("fragment3").source(Map.of("field3", "value3")));

        // Reference all fragments in one document
        bulkRequest.add(
            new IndexRequest().index("index")
                .id("1")
                .source(Map.of("field4", "value4"))
                .fragmentIds(List.of("fragment1", "fragment2", "fragment3"))
        );

        BulkResponse bulkResponse = internalCluster().coordOnlyNodeClient().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(false));

        // Verify all fragment fields were merged correctly
        assertResponse(prepareSearch("index"), response -> {
            assertHitCount(response, 1L);
            Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
            assertThat(source.get("field1"), equalTo("value1"));
            assertThat(source.get("field2"), equalTo("value2"));
            assertThat(source.get("field3"), equalTo("value3"));
            assertThat(source.get("field4"), equalTo("value4"));
        });
    }

    public void testConflictingFieldValues() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        // Add a fragment with a field
        bulkRequest.add(new FragmentRequest().index("index").id("fragment1").source(Map.of("conflicting_field", "fragment_value")));

        // Add a document with the same field but different value
        bulkRequest.add(
            new IndexRequest().index("index")
                .id("1")
                .source(Map.of("conflicting_field", "document_value"))
                .fragmentIds(List.of("fragment1"))
        );

        BulkResponse bulkResponse = internalCluster().coordOnlyNodeClient().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(false));

        // Verify which value takes precedence (document value should override fragment value)
        assertResponse(prepareSearch("index"), response -> {
            assertHitCount(response, 1L);
            Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
            assertThat(source.get("conflicting_field"), equalTo("document_value"));
        });
    }
}
