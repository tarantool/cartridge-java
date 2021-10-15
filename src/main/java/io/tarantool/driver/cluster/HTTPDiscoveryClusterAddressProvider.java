package io.tarantool.driver.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.CharsetUtil;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.exceptions.TarantoolClientException;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Tarantool server address provider with service discovery via HTTP.
 * Gets list of nodes from API endpoint in json format.
 *
 * Expected response format example:
 * <pre>
 * <code>
 * {
 *     "4141912c-34b8-4e40-a17e-7a6d80345954": {
 *         "uuid": "898b4d01-4261-4006-85ea-a3500163cda0",
 *         "uri": "localhost:3304",
 *         "status": "healthy",
 *         "priority": 1
 *     },
 *     "36a1a75e-60f0-4400-8bdc-d93e2c5ca54b": {
 *         "uuid": "9a3426db-f8f6-4e9f-ac80-e263527a59bc",
 *         "uri": "localhost:3302",
 *         "status": "healthy",
 *         "priority": 1
 *     }
 * }
 * </code>
 * </pre>
 *
 * Tarantool cartridge application lua http endpoint example:
 * <pre>
 * <code>
 *  ...
 *  local function get_routers()
 *    local cartridge = require('cartridge')
 *    local function table_contains(table, element)
 *      for _, value in pairs(table) do
 *        if value == element then
 *          return true
 *        end
 *      end
 *      return false
 *    end
 *
 *    local servers, err = cartridge.admin_get_servers()
 *    local routers = {}
 *
 *    for _, server in pairs(servers) do
 *      if server.replicaset ~= nil then
 *        if table_contains(server.replicaset.roles, 'app.roles.custom') then
 *          routers[server.uuid] = {
 *              status = server.healthy,
 *              uuid = server.uuid,
 *              uri = server.uri,
 *              priority = server.priority
 *          }
 *        end
 *      end
 *    end
 *
 *    return routers
 *  end
 *
 *  local httpd = cartridge.service_get('httpd')
 *
 *  local vshard = require('vshard')
 *  httpd:route({method = 'GET', path = '/endpoints'}, function(req)
 *      local json = require('json')
 *      local result = get_routers();
 *      return {body = json.encode(result)}
 *  end)
 *  ...
 * </code>
 * </pre>
 *
 * @author Sergey Volgin
 */
public class HTTPDiscoveryClusterAddressProvider extends AbstractDiscoveryClusterAddressProvider {

    private URI uri;
    private int port;
    private String host;
    private String scheme;

    private final SslContext sslContext;
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;

    public HTTPDiscoveryClusterAddressProvider(TarantoolClusterDiscoveryConfig config) {
        super(config);

        HTTPClusterDiscoveryEndpoint endpoint = (HTTPClusterDiscoveryEndpoint) config.getEndpoint();
        try {
            parseUri(endpoint.getUri());

            if ("https".equalsIgnoreCase(scheme)) {
                sslContext = SslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            } else {
                sslContext = null;
            }
        } catch (URISyntaxException | SSLException e) {
            throw new TarantoolClientException("Incorrect url %s, %s", endpoint.getUri(), e.getMessage());
        }

        this.eventLoopGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap()
                .group(this.eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, endpoint.getConnectTimeout());
        startDiscoveryTask();
    }

    /*
     * Quick and dirty solution. TODO rewrite with using the standard URI class
     */
    private void parseUri(final String uri) throws URISyntaxException, TarantoolClientException {
        this.uri = new URI(uri);
        this.scheme = this.uri.getScheme() == null ? "http" : this.uri.getScheme();
        this.host = this.uri.getHost() == null ? "127.0.0.1" : this.uri.getHost();
        this.port = this.uri.getPort();

        if (port == -1) {
            if ("http".equalsIgnoreCase(scheme)) {
                port = 80;
            } else if ("https".equalsIgnoreCase(scheme)) {
                port = 443;
            }
        }

        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
            throw new TarantoolClientException("Only HTTP(S) is supported. (%s)", uri);
        }
    }

    protected Collection<TarantoolServerAddress> discoverAddresses() {
        try {
            CompletableFuture<Map<String, ServerNodeInfo>> completableFuture = sendRequest();
            Map<String, ServerNodeInfo> addressMap = completableFuture.get();

            return addressMap.values().stream()
                    .filter(ServerNodeInfo::isAvailable)
                    .map(v -> new TarantoolServerAddress(v.getUri())).collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException("Cluster discovery task error", e);
        }
    }

    private CompletableFuture<Map<String, ServerNodeInfo>> sendRequest() throws InterruptedException {
        CompletableFuture<Map<String, ServerNodeInfo>> completableFuture = new CompletableFuture<>();

        TarantoolClusterDiscoveryConfig config = getDiscoveryConfig();
        HTTPClusterDiscoveryEndpoint endpoint = (HTTPClusterDiscoveryEndpoint) config.getEndpoint();
        getExecutorService().schedule(() -> {
            if (!completableFuture.isDone()) {
                completableFuture.completeExceptionally(new TimeoutException(String.format(
                        "Failed to get response for request in %d ms", endpoint.getReadTimeout())));
            }
        }, endpoint.getReadTimeout(), TimeUnit.MILLISECONDS);

        Bootstrap bootstrap = this.bootstrap.clone()
                .handler(new SimpleHttpClientInitializer(sslContext, completableFuture));

        Channel ch = bootstrap.connect(host, port).sync().channel();

        HttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath(), Unpooled.EMPTY_BUFFER);
        request.headers().set(HttpHeaderNames.HOST, host);
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);

        ch.writeAndFlush(request);
        ch.closeFuture().sync();
        return completableFuture;
    }

    @Override
    public void close() {
        super.close();
        try {
            eventLoopGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            throw new TarantoolClientException("Interrupted while shutting down the discovery service");
        }
    }

    private static class SimpleHttpClientInitializer extends ChannelInitializer<SocketChannel> {

        private CompletableFuture<Map<String, ServerNodeInfo>> completableFuture;
        private final SslContext sslCtx;

        SimpleHttpClientInitializer(SslContext sslCtx,
                                    CompletableFuture<Map<String, ServerNodeInfo>> completableFuture) {
            this.sslCtx = sslCtx;
            this.completableFuture = completableFuture;
        }

        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();

            if (sslCtx != null) {
                p.addLast(sslCtx.newHandler(ch.alloc()));
            }

            p.addLast(new HttpClientCodec());
            p.addLast(new HttpContentDecompressor());
            p.addLast(new HttpObjectAggregator(1048576));
            p.addLast(new SimpleHttpClientHandler(completableFuture));
        }
    }

    private static class SimpleHttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {

        private CompletableFuture<Map<String, ServerNodeInfo>> completableFuture;
        private ObjectMapper objectMapper = new ObjectMapper();

        SimpleHttpClientHandler(CompletableFuture<Map<String, ServerNodeInfo>> completableFuture) {
            super();
            this.completableFuture = completableFuture;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                String contentString = content.content().toString(CharsetUtil.UTF_8);

                TypeReference<HashMap<String, ServerNodeInfo>> typeReference =
                        new TypeReference<HashMap<String, ServerNodeInfo>>() {
                        };

                Map<String, ServerNodeInfo> responseMap;
                try {
                    responseMap = objectMapper.readValue(contentString, typeReference);
                } catch (Exception e) {
                    throw new TarantoolClientException("Cluster discovery task error", e);
                }

                completableFuture.complete(responseMap);

                if (content instanceof LastHttpContent) {
                    ctx.close();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
            completableFuture.completeExceptionally(cause);
        }
    }
}
