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
import io.tarantool.driver.ServerAddress;
import io.tarantool.driver.exceptions.TarantoolClientException;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Tarantool HTTP service discovery client.
 * Gets list of vshard-storage from api endpoint in json format.
 * <p>
 * Expected response format example:
 * <pre>
 *     <code>
 * {
 *     "4141912c-34b8-4e40-a17e-7a6d80345954": {
 *         "uuid": "898b4d01-4261-4006-85ea-a3500163cda0",
 *         "uri": "admin@localhost:3304",
 *         "status": "available",
 *         "network_timeout": 0.5
 *     },
 *     "36a1a75e-60f0-4400-8bdc-d93e2c5ca54b": {
 *         "uuid": "9a3426db-f8f6-4e9f-ac80-e263527a59bc",
 *         "uri": "admin@localhost:3302",
 *         "status": "available",
 *         "network_timeout": 0.5
 *     }
 * }
 *     </code>
 * </pre>
 * </p>
 * <p>
 * Tarantool cartridge application lua http endpoint example:
 * <pre>
 * <code>
 *  ...
 *      local function get_replica_set()
 *          local vshard = require('vshard')
 *          local router_info, err = vshard.router.info()
 *          if err ~= nil then
 *            error(err)
 *          end
 *
 *          local result = {}
 *          for i, v in pairs(router_info['replicasets']) do
 *              result[i] = v['master']
 *          end
 *          return result
 *      end
 *
 *      local httpd = cartridge.service_get('httpd')
 *
 *      local vshard = require('vshard')
 *      httpd:route({method = 'GET', path = '/endpoints'}, function(req)
 *          local json = require('json')
 *          local result = get_replica_set();
 *          return {body = json.encode(result)}
 *      end)
 *  ...
 * </code>
 * </pre>
 * </p>
 *
 * @author Sergey Volgin
 */
public class HTTPClusterDiscoverer implements ClusterDiscoverer {

    private URI uri;
    private int port;
    private String host;
    private String scheme;

    private final SslContext sslContext;
    private EventLoopGroup eventLoopGroup;
    private Bootstrap bootstrap;

    /**
     * Base constructor
     *
     * @param endpoint a {@link HTTPClusterDiscoveryEndpoint}
     * @throws TarantoolClientException if uri is incorrect
     */
    public HTTPClusterDiscoverer(HTTPClusterDiscoveryEndpoint endpoint, int connectionTimeout) {

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
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout);
    }

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

    public CompletableFuture<Map<String, ServerNodeInfo>> sendRequest() throws InterruptedException {
        CompletableFuture<Map<String, ServerNodeInfo>> completableFuture = new CompletableFuture<>();

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
        try {
            eventLoopGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<ServerAddress> getNodes() {
        try {
            CompletableFuture<Map<String, ServerNodeInfo>> completableFuture = sendRequest();
            Map<String, ServerNodeInfo> addressMap = completableFuture.get();

            return addressMap.values().stream()
                    .filter(v -> v.getStatus().equals("available"))
                    .map(v -> new ServerAddress(v.getUri())).collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException ignored) {
        }
        return null;
    }

    private static class SimpleHttpClientInitializer extends ChannelInitializer<SocketChannel> {

        private CompletableFuture<Map<String, ServerNodeInfo>> completableFuture;
        private final SslContext sslCtx;

        public SimpleHttpClientInitializer(SslContext sslCtx, CompletableFuture<Map<String, ServerNodeInfo>> completableFuture) {
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

        public SimpleHttpClientHandler(CompletableFuture<Map<String, ServerNodeInfo>> completableFuture) {
            super();
            this.completableFuture = completableFuture;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                String contentString = content.content().toString(CharsetUtil.UTF_8);

                TypeReference<HashMap<String, ServerNodeInfo>> typeReference =
                        new TypeReference<HashMap<String, ServerNodeInfo>>() {};

                Map<String, ServerNodeInfo> responseMap;
                try {
                    responseMap = objectMapper.readValue(contentString, typeReference);
                } catch (Exception ignored) {
                    //TODO: logger
                    responseMap = null;
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
