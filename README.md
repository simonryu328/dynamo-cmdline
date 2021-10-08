# dynamo-cmdline
>  Python Command-Line Interface Package to copy Dynamodb data in parallel batch processing + query natural & Global Secondary Indexes (GSIs).
#### Author: Simon Ryu

## What is DynamoDB?
Dynamo is a NoSQL database. A table in Dynamo is defined by its **Partition Key**, which
uniquely identifies a list of records.
The lists are ordered by the **Sort Key**, an optional key that along with Partition Key, form a 
primary key referred to as a *composite primary key*. This gives an additional flexibility when querying data.
Each partition (Partition Key) can be thought of as a filing cabinet drawer, containing a bunch of related records
which may or may not be sorted (Sort Key) depending on your need. Accounting for this optionality, the DynamodbTable class in module `dynamodb_table.py`
can represent tables with either simple (Partition Key) or composite (Partition Key + Sort Key) primary key.
\
\

## Dynamo vs Relational database
Dynamo differs from traditional, relational databases in that tables cannot be queried by random fields.
Because it is structured to guarantee fast and scalable queries, tables also cannot be joined, grouped or unioned.
A specific item can be found by specifiying a partition key and sort key, or a range of values within a partition,
filtered by the sort key. Although filtering by other fields (attributes) is possible,
it is highly discouraged as AWS charges the user based on how much data is read, and non-key filters occur *after*
the reads happen. Querying, and especially copying large amount of data across different AWS environments must be done with extreme care
(double the query operations!), hence this CLI package was distributed.
\
\

## GSI
Since querying is limited to the table's primary key, how can we address many different access patterns? The answer is global secondary indexes, or GSIs. A GSI allows you to essentially re-declare your table with a new key schema. When an item is written into the table, the index will update automatically, so managing dual-writing is not a concern. Most importantly, the GSI can be queried directly just like the natural table, just as fast.
\
\

### Libraries Used
- AWS SDK for Python - Boto3
- multiprocessing

