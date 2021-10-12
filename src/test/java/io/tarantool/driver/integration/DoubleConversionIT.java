package io.tarantool.driver.integration;

import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.api.DefaultTarantoolTupleFactory;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author Oleg Kuznetsov
 */
public class DoubleConversionIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
            new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    @Test
    public void test_crudSelect_shouldReturnTupleWithDouble() throws Exception {
        //given
        ProxyTarantoolTupleClient client = setupClient();
        client.space("test_space_with_double_field")
                .insert(tupleFactory.create(1, 1, Double.MAX_VALUE, 2.4)).get();

        //when
        TarantoolTuple fields = client
                .space("test_space_with_double_field")
                .select(Conditions.equals("id", 1)).get().get(0);

        //then
        Assertions.assertNotNull(fields);
        Assertions.assertEquals(Double.MAX_VALUE, fields.getDouble("double_field"));
    }

    @Test
    public void test_crudSelect_shouldReturnTupleWithDouble_ifSavedDoubleImplicit() throws Exception {
        //given
        ProxyTarantoolTupleClient client = setupClient();

        //when
        TarantoolTuple tuple = client.space("test_space_with_double_field")
                .insert(tupleFactory.create(2, 1, 2.4, 2.4)).get().get(0);

        //then
        Assertions.assertNotNull(tuple);
        Assertions.assertEquals(2.4D, tuple.getDouble("double_field"));
    }

    @Test
    public void test_insert_shouldSaveFloatFieldWithBadAccuracy_ifSavedInDoubleField() throws Exception {
        //given
        ProxyTarantoolTupleClient client = setupClient();

        //when
        TarantoolTuple tuple = client.space("test_space_with_double_field")
                .insert(tupleFactory.create(3, 1, 2.4f, 2.4f)).get().get(0);

        //then
        Assertions.assertNotNull(tuple);
        Double doubleField = tuple.getDouble("double_field");
        double actualTruncated = BigDecimal.valueOf(doubleField)
                .setScale(1, RoundingMode.HALF_DOWN)
                .doubleValue();

        Assertions.assertEquals(2.4d, actualTruncated);
        Assertions.assertEquals(2.4f, tuple.getDouble("number_field"));
    }

    @Test
    public void test_select_shouldReturnValuesAsUserExpected() throws Exception {
        //given
        ProxyTarantoolTupleClient client = setupClient();

        //when
        TarantoolTuple tuple = client.space("test_space_with_double_field")
                .insert(tupleFactory.create(4, 1, 1.1, 1)).get().get(0);

        //then
        Assertions.assertNotNull(tuple);
        Assertions.assertEquals(1.1D, tuple.getDouble("double_field"));
        Assertions.assertEquals(1, tuple.getInteger("double_field"));
        Assertions.assertEquals(1.1F, tuple.getFloat("double_field"));

        Assertions.assertEquals(1D, tuple.getDouble("number_field"));
        Assertions.assertEquals(1, tuple.getInteger("number_field"));
        Assertions.assertEquals(1F, tuple.getFloat("number_field"));

        Assertions.assertEquals(Integer.valueOf("1"), tuple.getObject("double_field", Integer.class).get());
        Assertions.assertEquals(Long.valueOf("1"), tuple.getObject("double_field", Long.class).get());
    }

    private ProxyTarantoolTupleClient setupClient() {
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(1000)
                .withReadTimeout(1000)
                .build();

        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
                config, container.getHost(), container.getPort());
        return new ProxyTarantoolTupleClient(clusterClient);
    }
}
