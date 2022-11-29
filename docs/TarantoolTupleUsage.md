[Main page](../README.md)

# TarantoolTuple usage

You can use TarantoolTuple for creating tuple which can be sent to the Tarantool instance
or can be returned from default "crud" functions.
You can create TarantoolTuple with this factory `TarantoolTupleFactory`.
See an example below:

```java
// Create a mapper factory  
DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
// Create a tuple factory
TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

// Create a tuple from listed values: [1,2.0,'3',4]
TarantoolTuple tarantoolTuple = tupleFactory.create(1, 2.0, "3", new BigDecimal("4"));

Optional<?> object = tarantoolTuple.getObject(0);
Optional<Integer> integer = tarantoolTuple.getObject(0, Integer.class);

// Returned value will have 'double' type (it is used by default). 
Optional<?> doubleValue = tarantoolTuple.getObject(1);
// To get 'float' value we must explicitly define the target type.
Optional<?> floatValue = tarantoolTuple.getObject(1, Float.class);

Optional<?> stringValue = tarantoolTuple.getObject(2);

Optional<?> bigDecimalValue = tarantoolTuple.getObject(3);
```
