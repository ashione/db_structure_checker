DB Structure Checker
====
## How to install
You should install pip first and execute 
```
pip install -r ./requirements.txt
```

## Example
```
# define reference object first
    nova_origin = DBConfig('root','ntse','s.zuolx.com','nova')
```
```
# then get variant db object
    nova_variant = DBConfig('root','ntse','s.zuolx.com','nova_variant')
# equal function was overrided to check four components.
# Equality wil return True, otherwise False
    # nova_origin == nova_variant

```
    print nova_origin.compare_table_name(nova_variant)
    print nova_origin.compare_table_column(nova_variant)
    print nova_origin.compare_table_indexes(nova_variant)
    print nova_origin.compare_table_fk(nova_variant)
```

**Differences will be displayed on terminal in RED color**
