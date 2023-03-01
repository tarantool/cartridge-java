[Main page](../README.md)

# How to set schema and do not get space not found error

If you create spaces only on storages inside a storage role or any other way, and you want to get data using request to
router. You will receive an error message like space not found.  
If you go directly to a storage you won't get this kind of error if spaces was created properly.   
But if you want to use CRUD to retrieve data from the router, there are some solutions.  
We recommend to set the scheme using migrations, they will be applied not only to storages, but also to routers.  
This way the router will be aware of the current scheme and such errors will not occur.  
Also you can proxy ddl.get_schema from storage to routers this should also fix your problem. But migrations are preferred.