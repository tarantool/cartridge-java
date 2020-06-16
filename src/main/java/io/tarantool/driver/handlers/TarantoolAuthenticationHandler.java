package io.tarantool.driver.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.tarantool.driver.auth.TarantoolAuthenticator;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.TarantoolVersionHolder;
import io.tarantool.driver.protocol.requests.TarantoolAuthRequest;

/**
 * Reads the greeting received from Tarantool server and optionally sends an authentication request with passed
 * credentials.
 *
 * @author Alexey Kuzin
 */
public class TarantoolAuthenticationHandler<S extends TarantoolCredentials, T extends TarantoolAuthenticator<S>>
        extends SimpleChannelInboundHandler<ByteBuf> {

    private static final int TARANTOOL_GREETING_LENGTH = 128; // bytes

    private final TarantoolVersionHolder versionHolder;
    private final S credentials;
    private final T authenticator;

    /**
     * Basic constructor.
     * @param versionHolder reads and holds the Tarantool server version received in the greeting
     * @param credentials Tarantool user credentials for authentication
     * @param authenticator an instance of {@link TarantoolAuthenticator} implementing authentication mechanism
     */
    public TarantoolAuthenticationHandler(TarantoolVersionHolder versionHolder, S credentials, T authenticator) {
        this.versionHolder = versionHolder;
        this.credentials = credentials;
        this.authenticator = authenticator;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in.readableBytes() < TARANTOOL_GREETING_LENGTH) {
            return; // skip until the next bytes will be read
        }
        String greeting = new String(in.readBytes(64).array());
        versionHolder.readVersion(greeting);
        if (authenticator.canAuthenticateWith(credentials)) {
            byte[] authData = authenticator.prepareUserAuthData(in.readBytes(64).array(), credentials);
            TarantoolAuthRequest authRequest = new TarantoolAuthRequest.Builder()
                    .withUsername(credentials.getUsername())
                    .withAuthData(authenticator.getMechanism(), authData).build();
            ctx.writeAndFlush(authRequest);
        }
        ctx.pipeline().remove(this); // authorize once per channel
    }
}
