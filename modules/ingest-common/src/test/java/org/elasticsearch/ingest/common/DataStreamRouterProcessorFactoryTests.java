/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;

public class DataStreamRouterProcessorFactoryTests extends ESTestCase {

    public void testSuccess() throws Exception {
        DataStreamRouterProcessor processor = create(null, null);
        assertThat(processor.getDataStreamDataset(), nullValue());
        assertThat(processor.getDataStreamNamespace(), nullValue());
    }

    public void testInvalidDataset() throws Exception {
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> create("my-service", null));
        assertThat(e.getMessage(), Matchers.equalTo("[dataset] contains illegal characters"));
    }

    public void testInvalidNamespace() throws Exception {
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> create("generic", "foo:bar"));
        assertThat(e.getMessage(), Matchers.equalTo("[namespace] contains illegal characters"));
    }

    private static DataStreamRouterProcessor create(String dataset, String namespace) throws Exception {
        Map<String, Object> config = new HashMap<>();
        if (dataset != null) {
            config.put("dataset", dataset);
        }
        if (namespace != null) {
            config.put("namespace", namespace);
        }
        return new DataStreamRouterProcessor.Factory().create(null, null, null, config);
    }
}
