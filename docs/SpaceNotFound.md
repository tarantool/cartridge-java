[Main page](../README.md)

# How to set schema and do not get "space not found" error?

## About problem
If you create spaces only on storages inside a storage role [like this](https://github.com/tarantool/cartridge-java/blob/ba6e8b248b6f448cc9f29e4d660a3d0a2f210ccd/src/test/resources/cartridge/app/roles/api_storage.lua#L12-L220)
or any other way, and you want to get data using router. You will receive an error message like "space not found".  
This happens because router does not have information about this space.  
If you work directly with a storage you won't get this kind of error if spaces was created properly.  
But if you want to use [CRUD](https://github.com/tarantool/crud) to retrieve data from the router, there are some solutions.  

## Migrations
We recommend to set the scheme using migrations, they will be applied not only to storages, but also to routers.  
This way the router will be aware of the current scheme and such errors will not occur.
Migrations will create spaces on both storages and routers.  
Tarantool CE supports migrations only since 2.2.x.
[Tarantool Enterprise Edition](https://www.tarantool.io/ru/product/enterprise/) supports migrations since 1.10.x and 2.2.x.
More information about migrations [tarantool/migrations](https://github.com/tarantool/migrations).  

## Proxying ddl.get_schema
If you want to create spaces only in storages, there is another solution.
Set up proxying ddl.get_schema from storage to routers. You should add this function to router role:
https://github.com/tarantool/cartridge-java/blob/ba6e8b248b6f448cc9f29e4d660a3d0a2f210ccd/src/test/resources/cartridge/app/roles/api_router.lua#L10-L14
And make it global.
https://github.com/tarantool/cartridge-java/blob/ba6e8b248b6f448cc9f29e4d660a3d0a2f210ccd/src/test/resources/cartridge/app/roles/api_router.lua#L228
Or you can just remove word `local` from line 10. Effect will be the same.
Now then cartridge-java tries to get schema from router 
