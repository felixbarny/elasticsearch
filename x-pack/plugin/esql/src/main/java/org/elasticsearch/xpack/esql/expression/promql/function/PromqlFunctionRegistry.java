/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

public class PromqlFunctionRegistry {
    public static final EsqlFunctionRegistry INSTANCE = new PromqlFunctionRegistry();
}
