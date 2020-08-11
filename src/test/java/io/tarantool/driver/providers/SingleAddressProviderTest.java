package io.tarantool.driver.providers;

import io.tarantool.driver.ServerAddress;
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
        ServerAddress serverAddress = new ServerAddress("10.0.2.15", 3301);
        SingleAddressProvider provider = new SingleAddressProvider(serverAddress);

        assertEquals("10.0.2.15", provider.getAddress().getHost());
        assertEquals("10.0.2.15", provider.getNext().getHost());
        assertEquals("10.0.2.15", provider.getNext().getHost());

        List<ServerAddress> addressList = Arrays.asList(
                new ServerAddress("127.0.0.1", 3301),
                new ServerAddress("127.0.0.2", 3301),
                new ServerAddress("127.0.0.3", 3301)
        );

        provider.updateAddressList(addressList);
        assertEquals("127.0.0.1", provider.getAddress().getHost());
        assertEquals("127.0.0.1", provider.getNext().getHost());

        assertThrows(IllegalArgumentException.class, () -> provider.updateAddressList(null));
        assertThrows(IllegalArgumentException.class, () -> provider.updateAddressList(new ArrayList<>()));
    }
}
