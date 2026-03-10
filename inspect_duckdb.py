import duckdb

print('module', duckdb)
print('attrs', dir(duckdb))
print('has_connect', 'connect' in dir(duckdb))
print('location', getattr(duckdb, '__file__', None))
