package = 'testapp'
version = 'scm-1'
source = {
    url = '/dev/null',
}
-- Put any modules your app depends on here
dependencies = {
    'tarantool',
    'lua >= 5.1',
    'cartridge == 2.8.3-1',
    'crud == 1.3.0-1',
}
build = {
    type = 'none';
}
