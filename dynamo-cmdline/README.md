# Dynamo CLI
>  Python Command-line interface to copy Dynamodb data in parallel batch processing + query natural & Global Secondary Indexes (GSIs).
#### Author: Simon Ryu

### Pre-requisite

- Profiles in `.aws/credentials` are configured for AWS environments (accounts).
- Install `dynamo-cmdline` package: ```pip install dynamo-cmdline```

## Running the CLI

#### Help message
> Displays a help message for using the CLI.


`$ dynamo -h`
```
Command line interface to copy, query and restore DynamoDB tables and items

positional arguments:
  {copy,restore,query}
    copy                Copy table or items from source to target envrionment
    query               Query items in specified environment's table

optional arguments:
  -h, --help            show this help message and exit
```

#### Copy table
> Creates an on-demand target table backup, truncates all items in the target table and copies all source table items in its place.


```$ dynamo copy --table foo --source production --target development```


Copy `foo` table from `production` to `development` environment.


#### Copy items
> Copies queried items from source table to target table. Queries items in source and target table, deletes them from the target table, then copies queried source items to the target table.
> For querying with sort keys, the comparision condition is begins_with.


```
$ dynamo copy --table foo --pk pkexample#id --sk skexample#id --source development --target test
```

Copy queried items in its natural table from development to test environment.


```$ dynamo copy --table foo --pk pkexample#id --index example-index --source prod --target stage```


Copy queried items in the table's secondary index from production to staging environment.

#### Query
> Query items in specified environment's table.

```$ dynamo query --table foo pkexample#id --env dev --head```

Prints the number of items queried as well as the first item returned from the query.

```$ dynamo query --table foo pkexample#id --env dev --unique entity-type```

Prints the number of items queried as well as the unique attribute values of the specified field. Atrribute value must be str.