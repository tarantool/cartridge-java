[Main page](../README.md)

# Token authorization

Token authorization is now supported only in [Tarantool Data Grid](https://www.tarantool.io/en/datagrid/).  

After the [token is generated](https://www.tarantool.io/en/tdg/latest/administration/security/tokens/) it can be used
for authorization.

Token can be placed in `credentials` Map. After that you can add `credentials` as last argument of calling TDG function

The following is an example of authorization by token:
```java
String username = "tdg_service_user";
String password = "";
String host = "127.0.0.1";
int port = 3301;
int connections = 1;

TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient = TarantoolClientFactory.createClient()
    .withConnections(connections)
    .withAddress(host, port)
    .withCredentials(username, password)
    .withProxyMethodMapping()
    .build();

HashMap<String, Object> tuple = new HashMap<>();
tuple.put("key", 1);
tuple.put("value", "value");

String yourToken = "ab57db87-9f58-4942-8347-28435b13fef0";
Map<String, String> credentials = new HashMap<>();
credentials.put("token", yourToken);

// repository.put(type_name, obj, options, context, credentials)
tarantoolClient.callForSingleResult(
    "repository.put",
    Arrays.asList("YourSpace", tuple, Collections.emptyList(), Collections.emptyList(), credentials),
    ArrayList.class
).get();

tarantoolClient.close();
```
