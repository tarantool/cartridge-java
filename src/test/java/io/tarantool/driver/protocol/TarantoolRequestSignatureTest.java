package io.tarantool.driver.protocol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author Alexey Kuzin
 */
public class TarantoolRequestSignatureTest {

    @Test
    public void testEquals() {
        Map<String, TarantoolRequestSignatureTestCase> testCases = new HashMap<>();
        testCases.put(
            "two empty signatures should be equal",
            new TarantoolRequestSignatureTestCase(
                new TarantoolRequestSignature(),
                new TarantoolRequestSignature(),
                true
            ));
        testCases.put(
            "empty and non-empty signatures should be not equal",
            new TarantoolRequestSignatureTestCase(
                new TarantoolRequestSignature(
                    "function", Arrays.asList(new Object[]{"param one", "param two"})),
                new TarantoolRequestSignature(),
                false
            ));
        testCases.put(
            "two signatures can be equal with different component contents",
            new TarantoolRequestSignatureTestCase(
                new TarantoolRequestSignature(
                    "function", Arrays.asList(new Object[]{"param one", "param two"})),
                new TarantoolRequestSignature(
                    "function", Arrays.asList(new Object[]{"param three", 4})),
                true
            ));
        testCases.put(
            "two signatures with different String components should not be equal",
            new TarantoolRequestSignatureTestCase(
                new TarantoolRequestSignature(
                    "function", Arrays.asList(new Object[]{"param one", "param two"})),
                new TarantoolRequestSignature(
                    "other_function", Arrays.asList(new Object[]{"param three", 4})),
                false
            ));
        testCases.put(
            "two signatures should not be equal with different component order",
            new TarantoolRequestSignatureTestCase(
                new TarantoolRequestSignature(
                    "function", Arrays.asList(new Object[]{"param one", "param two"})),
                new TarantoolRequestSignature(
                    Arrays.asList(new Object[]{"param three", 4}), "function"),
                false
            ));

        for (String testName: testCases.keySet()) {
            TarantoolRequestSignatureTestCase testCase = testCases.get(testName);
            if (testCase.equals) {
                assertEquals(testCase.first.hashCode(), testCase.second.hashCode(), testName);
                assertEquals(testCase.first, testCase.second, testName);
            } else {
                assertNotEquals(testCase.first.hashCode(), testCase.second.hashCode(), testName);
                assertNotEquals(testCase.first, testCase.second, testName);
            }
        }
    }

    @Test
    public void testSignatureAddComponent() {
        TarantoolRequestSignature signature = new TarantoolRequestSignature(
                    "function", Arrays.asList(new Object[]{"param one", "param two"}));
        TarantoolRequestSignature updatedSignature = new TarantoolRequestSignature(
                    "function", Arrays.asList(new Object[]{"param one", "param two"}));
        updatedSignature.addComponent("one more parameter");
        assertNotEquals(signature.hashCode(), updatedSignature.hashCode());
        assertNotEquals(signature, updatedSignature, "updated signature should not be equal to the source");
        int oldHashCode = updatedSignature.hashCode();
        updatedSignature.addComponent("last parameter");
        assertNotEquals(updatedSignature.hashCode(), oldHashCode);
    }

    static class TarantoolRequestSignatureTestCase {
        TarantoolRequestSignature first;
        TarantoolRequestSignature second;
        boolean equals;

        TarantoolRequestSignatureTestCase(
            TarantoolRequestSignature first, TarantoolRequestSignature second, boolean equals) {
            this.first = first;
            this.second = second;
            this.equals = equals;
        }
    }
}
