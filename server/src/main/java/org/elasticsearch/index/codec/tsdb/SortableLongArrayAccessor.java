/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.core.ScaledDecimals;

import java.util.Arrays;

public class SortableLongArrayAccessor implements ScaledDecimals.DoubleAccessor<long[]> {
    public static final SortableLongArrayAccessor INSTANCE = new SortableLongArrayAccessor();
    public static final long ZERO = NumericUtils.doubleToSortableLong(0.0);
    public static final long ONE = NumericUtils.doubleToSortableLong(1.0);

    public static SortableLongArrayAccessor get() {
        return INSTANCE;
    }

    @Override
    public double get(long[] target, int index) {
        return NumericUtils.sortableLongToDouble(target[index]);
    }

    @Override
    public void set(long[] target, int index, double d) {
        target[index] = NumericUtils.doubleToSortableLong(d);
    }

    @Override
    public int length(long[] target) {
        return target.length;
    }

    @Override
    public boolean isAllZeros(long[] target) {
        for (long v : target) {
            if (v != ZERO) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isAllOnes(long[] target) {
        for (long v : target) {
            if (v != ONE) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void fill(long[] target, int offset, int length, double value) {
        Arrays.fill(target, offset, offset + length, NumericUtils.doubleToSortableLong(value));

    }
}
