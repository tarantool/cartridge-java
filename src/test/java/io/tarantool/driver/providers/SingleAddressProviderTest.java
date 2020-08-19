package io.tarantool.driver.providers;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.cluster.SingleAddressProvider;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SingleAddressProviderTest {

    @Test
    public void getAddress() {
        TarantoolServerAddress tarantoolServerAddress = new TarantoolServerAddress("10.0.2.15", 3301);
        SingleAddressProvider provider = new SingleAddressProvider(tarantoolServerAddress);

        assertEquals("10.0.2.15", provider.getCurrentAddress().getHost());
        assertEquals("10.0.2.15", provider.getNextAddress().getHost());
        assertEquals("10.0.2.15", provider.getNextAddress().getHost());

        List<TarantoolServerAddress> addressList = Arrays.asList(
                new TarantoolServerAddress("127.0.0.1", 3301),
                new TarantoolServerAddress("127.0.0.2", 3301),
                new TarantoolServerAddress("127.0.0.3", 3301)
        );

        provider.updateAddressList(addressList);
        assertEquals("127.0.0.1", provider.getCurrentAddress().getHost());
        assertEquals("127.0.0.1", provider.getNextAddress().getHost());

        assertThrows(IllegalArgumentException.class, () -> provider.updateAddressList(null));
        assertThrows(IllegalArgumentException.class, () -> provider.updateAddressList(new ArrayList<>()));
    }
}
