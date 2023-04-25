[Main page](../README.md)

# Converters, mappers and mapper factories
In order to convert data from `MessagePack` types to basic Java types (`Integer`, `Double`, `String` and etc.) and back, you need mappers.
The mapper contains a stack of converters.
For each type , the converter is selected according to the following logic:
1) The mapper chooses the first matching converter from the top of the stack (the one that was added last).
2) Check that converter is suitable
    * The source type equals the input type of converter
    * `canConvertValue` or `canConvertObject`((depends on converter type see [creating your own converter](#creating-your-own-converter))) should return true
3) If this converter is not applicable, the mapper checks the next topmost converter after it.

## Using built-in mappers
See an example of `DefaultMessagePackMapper` usage:  
https://github.com/tarantool/cartridge-java/blob/d9ca853b6008137c2ea6c68a0246a68d43771d31/src/test/java/io/tarantool/driver/mappers/DefaultMessagePackMapperTest.java#L39-L82

Also, you can get the default mapper stack from `TarantoolClient`, change it, or use it:   
https://github.com/tarantool/cartridge-java/blob/d9ca853b6008137c2ea6c68a0246a68d43771d31/src/test/java/io/tarantool/driver/integration/ProxyTarantoolClientIT.java#L584-L592
Note: be careful changing the client mapper, it's used as the default mapper for API(`call`, `space(spaceName).select`, ...)

## Choosing the target type
If you want to manually choose the target type for a particular field in a tuple, use `get{targetTypeName}` methods
like `getInteger`, `getString` and others.  See the entire list in [TarantoolTuple interface](https://github.com/tarantool/cartridge-java/blob/master/src/main/java/io/tarantool/driver/api/tuple/TarantoolTuple.java)
https://github.com/tarantool/cartridge-java/blob/d9ca853b6008137c2ea6c68a0246a68d43771d31/src/test/java/io/tarantool/driver/integration/ClusterTarantoolTupleClientIT.java#L76-L87

## Creating your own converter
To create your own converter from Java basic types to `MessagePack` type you can implement `ValueConverter` interface and override `fromValue`, `canConvertValue` methods.
Let's look at the converter implementation. This is default `FloatValue` to `Float` converter (`FloatValue` is a `MessagePack` type):
https://github.com/tarantool/cartridge-java/blob/d9ca853b6008137c2ea6c68a0246a68d43771d31/src/main/java/io/tarantool/driver/mappers/converters/value/defaults/DefaultFloatValueToFloatConverter.java#L14-L32

If you need converter which will convert `MessagePack` type to Java basic type (`int`, `double`, `String` and etc.) you can implement `ObjectConverter` interface and
override `toValue`, `canConvertObject` methods:  
https://github.com/tarantool/cartridge-java/blob/d9ca853b6008137c2ea6c68a0246a68d43771d31/src/main/java/io/tarantool/driver/mappers/converters/object/DefaultFloatToFloatValueConverter.java#L13-L21

Default `canConvertObject` and `canConvertValue` implementation always returns `true`.
If any value of source type can be converted to target type you do not have to implement these methods.

## Adding converter to mapper
To add `MessagePack` type converter to the mapper use method `registerValueConverter`:  
https://github.com/tarantool/cartridge-java/blob/d9ca853b6008137c2ea6c68a0246a68d43771d31/src/test/java/io/tarantool/driver/mappers/DefaultMessagePackMapperTest.java#L156-L170

To add Java type converter use method `registerObjectConverter`:  
https://github.com/tarantool/cartridge-java/blob/d9ca853b6008137c2ea6c68a0246a68d43771d31/src/test/java/io/tarantool/driver/mappers/DefaultMessagePackMapperTest.java#L175-L187

After adding converter to the mapper this converter will have maximum priority
because it will be added to the top of the mapper stack.
Converters for container types (like lists and maps) can use mappers inside too, such converters (and mappers holding them) will be in fact recursive.

## Creating your own mapper
If you need to work with a custom structure of the received `MessagePack` objects, e.g. from a custom Lua method, use the mapper factory:  
https://github.com/tarantool/cartridge-java/blob/d9ca853b6008137c2ea6c68a0246a68d43771d31/src/test/java/io/tarantool/driver/integration/ProxyTarantoolClientIT.java#L525-L543
