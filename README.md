# dynamo-cmdline
>  Python Command Line Interface Package to copy Dynamodb data in parallel batch processing + query natural & Global Secondary Indexes (GSIs).
#### Author: Simon Ryu

This packaging distribution is published on PyPI, found [here](https://pypi.org/project/dynamo-cmdline/).

## Introduction
Maintaining multiple environments can be hard, especially on a cloud-based data model like Amazon's DynamoDB, where cost is dictated by data storage, read and write traffic. When I researched performing a cross-account DynamoDB data copy, I found many inflexible solutions such as [exporting the table to s3 and using AWS Glue job](https://aws.amazon.com/premiumsupport/knowledge-center/dynamodb-cross-account-migration/) or using [AWS Data Pipeline](https://aws.amazon.com/blogs/database/how-to-migrate-amazon-dynamodb-tables-from-one-aws-account-to-another-with-aws-data-pipeline/) that migrates an entire table with not much room for an optimized CI/CD. Instead I wanted to create a simple, light-weight interface for copying tables and subsets of items on a terminal. The operations had to be flexible and scalable, as I also wanted to integrate into CI/CD workflows such as my favourite [GitHub Actions](https://github.com/features/actions).
<br>
The application starts with an object-oriented approach: I used *boto3*, the AWS SDK for Python, to wrap a Dynamo table as a class where each instance represents a table in the AWS account. Then, I created a simple CLI with argparse to call class methods such as truncate, copy, restore and query as a high-level interaction with [AWS DynamoDB Client APIs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html). Item scans and writes were processed in parallel using *multiprocessing* and built-in [batch operations](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html), where any unprocessed items due to insufficient provisioned throughput were retried by implementing an exponential *backoff* algorithm. Finally the application was distributed as a package and implemented as a *GitHub Action Workflow* for copying subsets of data with a click of a button.

## What is DynamoDB?
Dynamo is a NoSQL database. A table in Dynamo is defined by its **Partition Key**, which
uniquely identifies a list of records.
The lists are ordered by the **Sort Key**, an optional key that along with Partition Key, form a 
primary key referred to as a *composite primary key*. This gives an additional flexibility when querying data.
Each partition (Partition Key) can be thought of as a filing cabinet drawer, containing a bunch of related records
which may or may not be sorted (Sort Key) depending on your need. Accounting for this optionality, the DynamodbTable class in module `dynamodb_table.py`
can represent tables with either simple (Partition Key) or composite (Partition Key + Sort Key) primary key.

## Dynamo vs Relational database
Dynamo differs from traditional, relational databases in that tables cannot be queried by random fields.
Because it is structured to guarantee fast and scalable queries, tables also cannot be joined, grouped or unioned.
A specific item can be found by specifiying a partition key and sort key, or a range of values within a partition,
filtered by the sort key. Although filtering by other fields (attributes) is possible,
it is highly discouraged as AWS charges the user based on how much data is read, and non-key filters occur *after*
the reads happen. Querying, and especially copying large amount of data across different AWS environments must be done with extreme care
(double the query operations!), hence this CLI package was distributed.

## GSI
Since querying is limited to the table's primary key, how can we address many different access patterns? The answer is global secondary indexes, or GSIs. A GSI allows you to essentially re-declare your table with a new key schema. When an item is written into the table, the index will update automatically, so managing dual-writing is not a concern. Most importantly, the GSI can be queried directly just like the natural table, just as fast.

### Libraries Used
- AWS SDK for Python - Boto3
- multiprocessing

