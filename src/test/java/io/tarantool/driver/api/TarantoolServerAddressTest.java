package io.tarantool.driver.api;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * @author Oleg Kuznetsov
 */
public class TarantoolServerAddressTest {

    @Test
    public void test_should_returnConcrete_Address() {
        //given
        final InetSocketAddress socketAddress = new InetSocketAddress("[::1]", 3301);

        //when
        final TarantoolServerAddress tarantoolServerAddress = new TarantoolServerAddress(socketAddress);

        //then
        assertEquals(socketAddress, tarantoolServerAddress.getSocketAddress());
    }

    @Test
    public void test_should_parseAddressWithUserPassword() {
        //given
        final String address = "test:test@localhost:3301";

        //when
        final TarantoolServerAddress tarantoolServerAddress = new TarantoolServerAddress(address);

        //then
        assertEquals("localhost", tarantoolServerAddress.getHost());
        assertEquals(3301, tarantoolServerAddress.getPort());
    }

    @Test
    public void test_should_parseAddressWithUser() {
        //given
        final String address = "test@localhost:3301";

        //when
        final TarantoolServerAddress tarantoolServerAddress = new TarantoolServerAddress(address);

        //then
        assertEquals("localhost", tarantoolServerAddress.getHost());
        assertEquals(3301, tarantoolServerAddress.getPort());
    }

    @Test
    public void test_should_parseIPv6Address() {
        //when
        final TarantoolServerAddress tarantoolServerAddress = new TarantoolServerAddress("[::1]", 3301);

        //then
        assertEquals("localhost", tarantoolServerAddress.getHost());
        assertEquals(3301, tarantoolServerAddress.getPort());
        assertEquals("localhost",
            tarantoolServerAddress.getSocketAddress().getAddress().getCanonicalHostName());
        assertEquals("0:0:0:0:0:0:0:1",
            tarantoolServerAddress.getSocketAddress().getAddress().getHostAddress());
    }

    @Test
    public void test_should_parseIPv6AddressWithPort() {
        //when
        final TarantoolServerAddress tarantoolServerAddress = new TarantoolServerAddress("[::1]:3301");

        //then
        assertEquals("localhost", tarantoolServerAddress.getHost());
        assertEquals(3301, tarantoolServerAddress.getPort());
        assertEquals("localhost",
            tarantoolServerAddress.getSocketAddress().getAddress().getCanonicalHostName());
        assertEquals("0:0:0:0:0:0:0:1",
            tarantoolServerAddress.getSocketAddress().getAddress().getHostAddress());
    }

    @Test
    public void test_should_parseIPv4Address() {
        //when
        final TarantoolServerAddress tarantoolServerAddress =
            new TarantoolServerAddress("127.0.0.1", 3301);

        //then
        assertEquals("localhost", tarantoolServerAddress.getHost());
        assertEquals(3301, tarantoolServerAddress.getPort());
        assertEquals("localhost",
            tarantoolServerAddress.getSocketAddress().getAddress().getCanonicalHostName());
        assertEquals("127.0.0.1",
            tarantoolServerAddress.getSocketAddress().getAddress().getHostAddress());
    }

    @Test
    public void test_should_parseLocalhost() {
        //when
        final TarantoolServerAddress tarantoolServerAddress =
            new TarantoolServerAddress("localhost", 3301);

        //then
        assertEquals("localhost", tarantoolServerAddress.getHost());
        assertEquals(3301, tarantoolServerAddress.getPort());
        assertEquals("localhost",
            tarantoolServerAddress.getSocketAddress().getAddress().getCanonicalHostName());
        assertEquals("127.0.0.1",
            tarantoolServerAddress.getSocketAddress().getAddress().getHostAddress());
    }

    @Test
    public void test_should_throwException_ifAddressIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new TarantoolServerAddress("user:password@:3301"));
    }

    @Test
    public void test_should_throwException_ifHostIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new TarantoolServerAddress("user:password@ "));
    }

    @Test
    public void test_should_throwException_ifPortIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new TarantoolServerAddress("user:password@localhost"));
    }

    @Test
    public void test_should_throwException_ifAddressIpv6IsNotCorrect() {
        assertThrows(IllegalArgumentException.class, () -> new TarantoolServerAddress("user:password@[::1:3301"));
    }

    @Test
    public void test_should_throwException_ifAddressIsNotCorrect() {
        assertThrows(IllegalArgumentException.class, () -> new TarantoolServerAddress("user@password@[::1]:3301"));
    }

    @Test
    public void test_should_throwException_ifHostIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new TarantoolServerAddress((String) null));
    }

    @Test
    public void test_should_throwException_ifPortIsIncorrect() {
        assertThrows(IllegalArgumentException.class, () -> new TarantoolServerAddress("user:password@[::1]:33o1"));
    }

    @Test
    public void test_should_createTarantoolServerAddressWithDefaultAddress() {
        final TarantoolServerAddress tarantoolServerAddress = new TarantoolServerAddress();

        assertEquals("localhost", tarantoolServerAddress.getHost());
        assertEquals(3301, tarantoolServerAddress.getPort());
    }

    @Test
    public void test_should_throwExceptionIfPortIsNotCorrect() {
        assertThrows(IllegalArgumentException.class, () -> new TarantoolServerAddress("localhost:10000000"));
    }

    @Test
    public void test_should_throwExceptionIfPortIsNegative() {
        assertThrows(IllegalArgumentException.class, () -> new TarantoolServerAddress("localhost:-3301"));
    }
}
