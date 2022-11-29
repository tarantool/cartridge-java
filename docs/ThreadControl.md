[Main page](../README.md)

# Thread control

For specific case maybe need specify custom numbers of netty work threads
First way is use client builder property

```java

TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> setupClient() {
        return TarantoolClientFactory.createClient()
        .withCredentials("admin", "secret-cluster-cookie")
        .withAddress(container.getRouterHost(), container.getRouterPort())
        .withEventLoopThreadsNumber(threadsNumber)
        .build();
```        

Second way is use java system property

```
-Dio.netty.eventLoopThreads
```        

All these  properties should be used carefully, because if threads more than need may give low app performance.
