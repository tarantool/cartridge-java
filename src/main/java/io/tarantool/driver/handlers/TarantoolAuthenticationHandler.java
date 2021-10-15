package io.tarantool.driver.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.tarantool.driver.TarantoolVersionHolder;
import io.tarantool.driver.auth.TarantoolAuthenticator;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.exceptions.TarantoolBadCredentialsException;
import io.tarantool.driver.protocol.requests.TarantoolAuthRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Reads the greeting received from Tarantool server and optionally sends an authentication request with passed
 * credentials.
 *
 * @author Alexey Kuzin
 */
public class TarantoolAuthenticationHandler<S extends TarantoolCredentials, T extends TarantoolAuthenticator<S>>
        extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(TarantoolAuthenticationHandler.class);
    private static final int VERSION_LENGTH = 64; // bytes
    private static final int SALT_LENGTH = 44; // bytes
    private static final int GREETING_LENGTH = 128; // bytes

    private final TarantoolVersionHolder versionHolder;
    private final S credentials;
    private final T authenticator;
    private final CompletableFuture<Channel> connectionFuture;

    /**
     * Basic constructor.
     * @param connectionFuture future for tracking the authentication progress
     * @param versionHolder reads and holds the Tarantool server version received in the greeting
     * @param credentials Tarantool user credentials for authentication
     * @param authenticator an instance of {@link TarantoolAuthenticator} implementing authentication mechanism
     */
    public TarantoolAuthenticationHandler(CompletableFuture<Channel> connectionFuture,
                                          TarantoolVersionHolder versionHolder,
                                          S credentials, T authenticator) {
        this.connectionFuture = connectionFuture;
        this.versionHolder = versionHolder;
        this.credentials = credentials;
        this.authenticator = authenticator;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in.readableBytes() < GREETING_LENGTH) {
            return; // skip until the next bytes will be read
        }
        byte[] array = new byte[VERSION_LENGTH];
        in.readBytes(array);
        String greeting = new String(array);
        versionHolder.readVersion(greeting);
        if (authenticator.canAuthenticateWith(credentials)) {
            array = new byte[SALT_LENGTH];
            in.readBytes(array).skipBytes(VERSION_LENGTH - SALT_LENGTH);
            byte[] authData = authenticator.prepareUserAuthData(array, credentials);
            TarantoolAuthRequest authRequest = new TarantoolAuthRequest.Builder()
                    .withUsername(credentials.getUsername())
                    .withAuthData(authenticator.getMechanism(), authData).build();
            ctx.channel().writeAndFlush(authRequest).addListener(f -> {
                if (!f.isSuccess()) {
                    connectionFuture.completeExceptionally(
                            new RuntimeException("Failed to write the auth request to channel", f.cause()));
                }
            });
        } else if (authenticator.canSkipAuth(credentials)) {
            log.info("Cannot authenticate with provided credentials, skipping authentication");
            connectionFuture.complete(ctx.channel());
            // remove response handler as well, because no auth response is expected
            ctx.pipeline().remove(TarantoolAuthenticationResponseHandler.class);
        } else {
            throw new TarantoolBadCredentialsException();
        }
        ctx.pipeline().remove(this); // authorize once per channel
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        connectionFuture.completeExceptionally(cause);
    }
}
