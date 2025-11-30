/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for the Decimal class.
 * These tests verify the Java port matches the behavior of the Go implementation.
 */
public class ScaledDecimalsTests extends ESTestCase {

    private static final double EPSILON = 1e-10;

    private ScaledDecimals.ScaledDecimal result;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        result = new ScaledDecimals.ScaledDecimal();
    }

    public void testFromDoubleSimple() {
        // Test integer conversion
        ScaledDecimals.fromDouble(123.0, result);
        assertThat(result.value, equalTo(123L));
        assertThat(result.exponent, equalTo((short) 0));

        // Test decimal conversion
        ScaledDecimals.fromDouble(123.456, result);
        assertThat(result.value, equalTo(123456L));
        assertThat(result.exponent, equalTo((short) -3));

        // Test zero
        ScaledDecimals.fromDouble(0.0, result);
        assertThat(result.value, equalTo(0L));
        assertThat(result.exponent, equalTo((short) 0));
    }

    public void testFromDoubleNegative() {
        ScaledDecimals.fromDouble(-123.456, result);
        assertThat(result.value, equalTo(-123456L));
        assertThat(result.exponent, equalTo((short) -3));
    }

    public void testFromDoubleSpecialValues() {
        // Test positive infinity
        ScaledDecimals.fromDouble(Double.POSITIVE_INFINITY, result);
        assertThat(result.value, equalTo(Long.MAX_VALUE));

        // Test negative infinity
        ScaledDecimals.fromDouble(Double.NEGATIVE_INFINITY, result);
        assertThat(result.value, equalTo(Long.MIN_VALUE));

        // Test stale NaN
        ScaledDecimals.fromDouble(ScaledDecimals.STALE_NAN, result);
        assertThat(ScaledDecimals.isStaleNaNInt64(result.value), is(true));
    }

    public void testToDouble() {
        // Test basic conversion
        double f = ScaledDecimals.toDouble(123456L, (short) -3);
        assertThat(f, closeTo(123.456, EPSILON));

        // Test zero exponent
        f = ScaledDecimals.toDouble(123L, (short) 0);
        assertThat(f, closeTo(123.0, EPSILON));

        // Test positive exponent
        f = ScaledDecimals.toDouble(123L, (short) 2);
        assertThat(f, closeTo(12300.0, EPSILON));

        // Test negative value
        f = ScaledDecimals.toDouble(-123456L, (short) -3);
        assertThat(f, closeTo(-123.456, EPSILON));
    }

    public void testRoundTrip() {
        double[] testValues = { 0.0, 1.0, -1.0, 123.456, -123.456, 0.001, 1000000.0, 3.14159265359, 1e-6, 1e6 };

        for (double original : testValues) {
            ScaledDecimals.fromDouble(original, result);
            double reconstructed = ScaledDecimals.toDouble(result.value, result.exponent);
            assertThat(reconstructed, closeTo(original, Math.abs(original) * 1e-12 + 1e-15));
        }
    }

    public void testAppendDecimalToDouble() {
        long[] decimals = { 100L, 200L, 300L };
        short exp = -1;
        double[] dst = new double[decimals.length];

        int written = ScaledDecimals.appendDecimalToDouble(dst, 0, decimals, exp);

        assertThat(written, equalTo(decimals.length));
        assertThat(dst[0], closeTo(10.0, EPSILON));
        assertThat(dst[1], closeTo(20.0, EPSILON));
        assertThat(dst[2], closeTo(30.0, EPSILON));
    }

    public void testAppendDecimalToDoubleZeroExponent() {
        long[] decimals = { 1L, 2L, 3L };
        short exp = 0;
        double[] dst = new double[decimals.length];

        ScaledDecimals.appendDecimalToDouble(dst, 0, decimals, exp);

        assertThat(dst[0], closeTo(1.0, EPSILON));
        assertThat(dst[1], closeTo(2.0, EPSILON));
        assertThat(dst[2], closeTo(3.0, EPSILON));
    }

    public void testAppendDecimalToDoublePositiveExponent() {
        long[] decimals = { 1L, 2L, 3L };
        short exp = 2;
        double[] dst = new double[decimals.length];

        ScaledDecimals.appendDecimalToDouble(dst, 0, decimals, exp);

        assertThat(dst[0], closeTo(100.0, EPSILON));
        assertThat(dst[1], closeTo(200.0, EPSILON));
        assertThat(dst[2], closeTo(300.0, EPSILON));
    }

    public void testAppendDoubleToDecimal() {
        double[] floats = { 1.1, 2.2, 3.3 };
        long[] dst = new long[floats.length];

        short exp = ScaledDecimals.appendDoubleToDecimal(dst, 0, floats);

        // Verify values are stored correctly
        assertThat(dst, notNullValue());
        assertThat(exp, lessThan((short) 0)); // Should have negative exponent for decimals

        // Verify round-trip
        double[] reconstructed = new double[dst.length];
        ScaledDecimals.appendDecimalToDouble(reconstructed, 0, dst, exp);

        for (int i = 0; i < floats.length; i++) {
            assertThat(reconstructed[i], closeTo(floats[i], EPSILON));
        }
    }

    public void testAppendDoubleToDecimalAllZeros() {
        double[] floats = { 0.0, 0.0, 0.0 };
        long[] dst = new long[floats.length];

        short exp = ScaledDecimals.appendDoubleToDecimal(dst, 0, floats);

        assertThat(exp, equalTo((short) 0));
        for (long v : dst) {
            assertThat(v, equalTo(0L));
        }
    }

    public void testAppendDoubleToDecimalAllOnes() {
        double[] floats = { 1.0, 1.0, 1.0 };
        long[] dst = new long[floats.length];

        short exp = ScaledDecimals.appendDoubleToDecimal(dst, 0, floats);

        assertThat(exp, equalTo((short) 0));
        for (long v : dst) {
            assertThat(v, equalTo(1L));
        }
    }

    public void testCalibrateScaleSameExponent() {
        long[] a = { 100L, 200L };
        long[] b = { 10L, 20L };
        short ae = 2;
        short be = 2;

        long[] aCopy = a.clone();
        long[] bCopy = b.clone();

        short result = ScaledDecimals.calibrateScale(a, ae, b, be);

        assertThat(result, equalTo(ae));
        // Arrays should be unchanged when exponents are equal
        assertThat(a, equalTo(aCopy));
        assertThat(b, equalTo(bCopy));
    }

    public void testCalibrateScaleDifferentExponents() {
        long[] a = { 10L, 20L };
        long[] b = { 100L, 200L };
        short ae = 1;
        short be = 0;

        short result = ScaledDecimals.calibrateScale(a, ae, b, be);

        // Result should be somewhere between ae and be
        assertThat(result >= be && result <= ae, is(true));

        // Values should be scaled appropriately
        // The exact values depend on the algorithm, but they should maintain relative scale
        assertThat(a[0], not(equalTo(10L))); // Should be scaled
    }

    public void testCalibrateScaleEmptyArrays() {
        long[] a = {};
        long[] b = { 10L, 20L };
        short ae = 1;
        short be = 0;

        short result = ScaledDecimals.calibrateScale(a, ae, b, be);
        assertThat(result, equalTo(be));

        a = new long[] { 10L, 20L };
        b = new long[] {};
        result = ScaledDecimals.calibrateScale(a, ae, b, be);
        assertThat(result, equalTo(ae));
    }

    public void testRoundToDecimalDigits() {
        double f = 123.456789;

        assertThat(ScaledDecimals.roundToDecimalDigits(f, 2), closeTo(123.46, EPSILON));
        assertThat(ScaledDecimals.roundToDecimalDigits(f, 3), closeTo(123.457, EPSILON));
        assertThat(ScaledDecimals.roundToDecimalDigits(f, 4), closeTo(123.4568, EPSILON));
        assertThat(ScaledDecimals.roundToDecimalDigits(f, 0), closeTo(123.0, EPSILON));
    }

    public void testRoundToDecimalDigitsNegative() {
        double f = 123.456;

        assertThat(ScaledDecimals.roundToDecimalDigits(f, -1), closeTo(120.0, EPSILON));
        assertThat(ScaledDecimals.roundToDecimalDigits(f, -2), closeTo(100.0, EPSILON));
    }

    public void testRoundToDecimalDigitsStaleNaN() {
        double stale = ScaledDecimals.STALE_NAN;
        double result = ScaledDecimals.roundToDecimalDigits(stale, 2);
        assertThat(ScaledDecimals.isStaleNaN(result), is(true));
    }

    public void testRoundToSignificantFigures() {
        double f = 123.456789;

        assertThat(ScaledDecimals.roundToSignificantFigures(f, 3), closeTo(123.0, EPSILON));
        assertThat(ScaledDecimals.roundToSignificantFigures(f, 4), closeTo(123.5, EPSILON));
        assertThat(ScaledDecimals.roundToSignificantFigures(f, 5), closeTo(123.46, EPSILON));
    }

    public void testRoundToSignificantFiguresSmallNumber() {
        double f = 0.0012345;

        assertThat(ScaledDecimals.roundToSignificantFigures(f, 2), closeTo(0.0012, EPSILON));
        assertThat(ScaledDecimals.roundToSignificantFigures(f, 3), closeTo(0.00123, EPSILON));
    }

    public void testRoundToSignificantFiguresStaleNaN() {
        double stale = ScaledDecimals.STALE_NAN;
        double result = ScaledDecimals.roundToSignificantFigures(stale, 3);
        assertThat(ScaledDecimals.isStaleNaN(result), is(true));
    }

    public void testRoundToSignificantFiguresSpecialValues() {
        assertEquals(0.0, ScaledDecimals.roundToSignificantFigures(0.0, 3), EPSILON);
        assertThat(Double.isInfinite(ScaledDecimals.roundToSignificantFigures(Double.POSITIVE_INFINITY, 3)), is(true));
        assertThat(Double.isInfinite(ScaledDecimals.roundToSignificantFigures(Double.NEGATIVE_INFINITY, 3)), is(true));
    }

    public void testIsStaleNaN() {
        assertThat(ScaledDecimals.isStaleNaN(ScaledDecimals.STALE_NAN), is(true));
        assertThat(ScaledDecimals.isStaleNaN(Double.NaN), is(false));
        assertThat(ScaledDecimals.isStaleNaN(0.0), is(false));
        assertThat(ScaledDecimals.isStaleNaN(Double.POSITIVE_INFINITY), is(false));
    }

    public void testIsStaleNaNInt64() {
        ScaledDecimals.fromDouble(ScaledDecimals.STALE_NAN, result);
        assertThat(ScaledDecimals.isStaleNaNInt64(result.value), is(true));

        ScaledDecimals.fromDouble(123.456, result);
        assertThat(ScaledDecimals.isStaleNaNInt64(result.value), is(false));
    }

    public void testLargeNumbers() {
        double large = 1e15;
        ScaledDecimals.fromDouble(large, result);
        double reconstructed = ScaledDecimals.toDouble(result.value, result.exponent);
        assertEquals(large, reconstructed, large * 1e-12);
    }

    public void testSmallNumbers() {
        double small = 1e-15;
        ScaledDecimals.fromDouble(small, result);
        double reconstructed = ScaledDecimals.toDouble(result.value, result.exponent);
        assertEquals(small, reconstructed, small * 1e-12 + 1e-25);
    }

    public void testReusabilityOfResultObjects() {
        // Verify that result objects can be reused without issues
        double[] testValues = { 1.1, 2.2, 3.3, 4.4, 5.5 };

        for (double value : testValues) {
            ScaledDecimals.fromDouble(value, result);
            double reconstructed = ScaledDecimals.toDouble(result.value, result.exponent);
            assertEquals(value, reconstructed, EPSILON);
        }
    }

    public void testBatchConversion() {
        // Test converting a batch of values
        double[] floats = new double[100];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = i * 0.1;
        }

        long[] decimals = new long[floats.length];
        short exp = ScaledDecimals.appendDoubleToDecimal(decimals, 0, floats);

        double[] reconstructed = new double[floats.length];
        ScaledDecimals.appendDecimalToDouble(reconstructed, 0, decimals, exp);

        for (int i = 0; i < floats.length; i++) {
            assertThat(reconstructed[i], closeTo(floats[i], EPSILON));
        }
    }

    public void testOffsetWriting() {
        // Test that offset parameter works correctly
        double[] dst = new double[10];
        long[] src = { 100L, 200L, 300L };

        int written = ScaledDecimals.appendDecimalToDouble(dst, 5, src, (short) -1);

        assertEquals(3, written);
        assertEquals(0.0, dst[4], EPSILON); // Before offset should be 0
        assertEquals(10.0, dst[5], EPSILON); // At offset
        assertEquals(20.0, dst[6], EPSILON);
        assertEquals(30.0, dst[7], EPSILON);
        assertEquals(0.0, dst[8], EPSILON); // After data should be 0
    }

    public void testEdgeCaseExponents() {
        // Test with very large positive exponent
        long[] decimals = { 1L };
        double[] dst = new double[1];
        ScaledDecimals.appendDecimalToDouble(dst, 0, decimals, (short) 10);
        assertEquals(1e10, dst[0], EPSILON);

        // Test with very large negative exponent
        ScaledDecimals.appendDecimalToDouble(dst, 0, decimals, (short) -10);
        assertEquals(1e-10, dst[0], EPSILON);
    }

    public void testNegativeValueConversions() {
        long[] decimals = { -100L, -200L, -300L };
        double[] dst = new double[decimals.length];

        ScaledDecimals.appendDecimalToDouble(dst, 0, decimals, (short) -1);

        assertEquals(-10.0, dst[0], EPSILON);
        assertEquals(-20.0, dst[1], EPSILON);
        assertEquals(-30.0, dst[2], EPSILON);
    }
}
