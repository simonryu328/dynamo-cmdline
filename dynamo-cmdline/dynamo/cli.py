#!/usr/bin/env python3

"""dynamo.cli: provides entry point main()."""

__version__ = "0.0.1"

import argparse
from .dynamodb_table import DynamodbTable
import json

def main():
    parser = argparse.ArgumentParser(description='Command line interface to copy, query and restore DynamoDB tables and items')
    subparsers = parser.add_subparsers(dest='command', required=True)
    copy_parser = subparsers.add_parser('copy', help='Copy table or items from source to target envrionment')
    query_parser = subparsers.add_parser('query', help="Query items in specified environment's table")

    copy_parser.add_argument('-t','--table', help='DynamoDB table to perform copy operations', required=True)
    copy_parser.add_argument('-pk','--pk', help='Partition key of the DynamoDB items to copy', required=False)
    copy_parser.add_argument('-sk','--sk', help='Optional; Secondary key of the DynamoDB items to copy', required=False)
    copy_parser.add_argument('-i','--index', help='Optional; Secondary index in which to query the items', required=False)
    copy_parser.add_argument('-src','--source', help='Source AWS environment to copy data from', required=True)
    copy_parser.add_argument('-tgt','--target', help='Target AWS environment to wipe and repopulate with copied data', required=True)

    query_parser.add_argument('-t','--table', help='DynamoDB table to query in', required=True)
    query_parser.add_argument('-pk','--pk', help='Partition key of the DynamoDB items to query', required=True)
    query_parser.add_argument('-sk','--sk', help='Optional; Secondary key of the DynamoDB items to query', required=False)
    query_parser.add_argument('-i','--index', help='Optional; Secondary index in which to query the items', required=False)
    query_parser.add_argument('-e','--env', help='AWS environment', required=True)
    query_parser.add_argument('-u','--unique', help='Returns the unique attribute values from the query. Enter an attribute name', required=False)
    query_parser.add_argument('-head','--head', help='Returns the first item JSON from the query', action='store_true', required=False)

    args = parser.parse_args()
    if args.command == 'copy':
        source_table = DynamodbTable(env=args.source, table_name=args.table)
        target_table = DynamodbTable(env=args.target, table_name=args.table)
        if args.pk is not None:
            source_table.copy_dynamodb_items(target_table, pk=args.pk, sk=args.sk, index_name=args.index)
        else:
            source_table.copy_dynamodb_table(target_table)

    elif args.command == 'query':
        table = DynamodbTable(env=args.env, table_name=args.table)
        items = table.query_items(pk=args.pk, sk=args.sk, index_name=args.index)
        if items:
            print(f"{len(items)} items queried.")
            if args.head:
                # Show the head of query result.
                print(json.dumps(items[0], indent=4))
                print('-'*30)

            if args.unique is not None:
                s = set()
                for item in items:
                    s.add(item[args.unique]['S'])
                print(s)
        else:
            print("No item was found.")

if __name__ == '__main__':
    main()

