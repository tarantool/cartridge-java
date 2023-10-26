package io.tarantool.driver;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static io.tarantool.driver.TarantoolUtils.versionGreaterOrEqualThen;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TarantoolUtilsTest {

    @ParameterizedTest(name = "[{index}] {0} >= {1} == {2}")
    @CsvSource(nullValues = "null", value = {
        "null , 2.1 , true",
        "'' , 2.1 , true",
        "2.2 , 2.1 , true",
        "2.2 , 2.2 , true",
        "2.2.1 , 2.2 , true",
        "2.2 , 2.2.1 , true",
        "2.2.1 , 2.2.2 , true",
        "2.x , 2.2 , true",
        "2.x , 2.x , true",
        "2.2 , 2.3 , false",
        "3.2 , 2.3 , true",
    })
    void test_minimumVersionCheck(String current, String minimum, boolean expected) {
        assertEquals(expected, versionGreaterOrEqualThen(current, minimum));
    }

}
