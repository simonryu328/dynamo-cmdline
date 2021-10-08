import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
import pathos.multiprocessing as mp
import time
from typing import List, Dict

# CONFIGS
BATCH_SIZE = 25

class DynamodbTable:
    """
    Public methods include: Create/restore an on-demand backup, copy table/items across different
    AWS environments, and query operations.

    Attributes:
        env: AWS environment of the table.
        table_name: Name of the table.
        pk_name: Partition key of the table.
        sk_name: Sort key of the table.
    """
    def __init__(self, env: str, table_name: str):
        self.env = env
        self.table_name = table_name
        self.pk_name, self.sk_name = self._get_primary_key()
        
    def __repr__(self):
        return f'DynamodbTable(env={self.env}, table_name={self.table_name}, pk_name={self.pk_name}, sk_name={self.sk_name})'

    def copy_dynamodb_table(self, other) -> None:
        """Copies source(self) table to target(other) table.

        Creates an on-demand target table backup, truncates all items in the target table 
        and copies all source table items in its place.
        
        Args:
            self: source table to copy.
            other: target table to put all copied items.
        Returns:
            None
        Raises:
            Exception: Cannot copy different tables: {self.table_name} != {other.table_name}.
        """
        # Allows copying from/to backup table, assuming name of the original table is included in its name.
        if self.table_name not in other.table_name and other.table_name not in self.table_name:
            raise Exception(f'Cannot copy different tables: {self.table_name} != {other.table_name}.')

        other.create_backup()
        other._truncate()
        self._copy_in_parallel_batch(other)

    def copy_dynamodb_items(self, other, pk: str, sk: str = None, index_name: str = None) -> None:
        """Copies queried items from source(self) table to target table(other).
        
        Queries items in source and target table, exports target items to S3, 
        deletes them from the target table, then copies queried source items to the target table.
        
        Args:
            self: source table to copy the queried items.
            other: target table to put the queried items.
            pk: Partition key of the item (value of the partition key).
            sk: Optional; Sort key of the item. 
            index_name: Optional; name of the secondary index.
        Returns:
            None
        Raises:
            Exception: Cannot copy items across tables with different names: {self.table_name} != {other.table_name}.
        """
        if self.table_name not in other.table_name and other.table_name not in self.table_name:
            raise Exception(f'Cannot copy items across tables with different names: {self.table_name} != {other.table_name}.')

        source_items = self.query_items(pk=pk, sk=sk, index_name=index_name)
        target_items = other.query_items(pk=pk, sk=sk, index_name=index_name)
        self._copy_items_in_parallel_batch(other, source_items, target_items)

    def query_items(self, pk: str, sk: str = None, index_name: str = None) -> List[Dict]:
        """Queries items in the table.

        For querying with sort keys, the comparision condition is begins_with,
        which is true if the sort key value begins with a particular operand.
        
        Args:
            pk: Partition key of the item.
            sk: Optional; Sort key of the item
            index_name: Optional; name of the secondary index.
        Returns:
            List of queried items.
        Raises:
            Exception: Query returned 0 result.
        """
        last_evaluated_key = None
        items = []
        params = {
            'TableName': self.table_name,
            'Limit': 200,
            'Select': 'ALL_ATTRIBUTES',
            'ExpressionAttributeValues': {
                ':item_key': {
                    'S': pk
                },
            }
        }

        if index_name is not None:
            pk_name, sk_name = self._get_secondary_key(index_name=index_name)
            params['IndexName'] = index_name

        else: pk_name, sk_name = self.pk_name, self.sk_name

        if sk is not None:
            params['KeyConditionExpression'] = f'{pk_name} = :item_key AND begins_with ( #D, :val )'
            params['ExpressionAttributeNames'] = {'#D':sk_name}
            params['ExpressionAttributeValues'][':val'] = {'S':sk}
        else:
            params['KeyConditionExpression'] = f'{pk_name} = :item_key'

        while True:
            if last_evaluated_key:
                params['ExclusiveStartKey'] = last_evaluated_key
            response = self._get_dynamodb_client().query(**params)
            items.extend(response['Items'])
            # For pagination without scanning again the same items.
            if 'LastEvaluatedKey' in response and 'ScannedCount' in response and response['ScannedCount'] > 0:
                last_evaluated_key = response['LastEvaluatedKey']
            else:
                break

        return items

    def create_backup(self) -> Dict:
        """Creates an on-demand backup of the table and returns the response."""
        response = self._get_dynamodb_client().create_backup(
            TableName=self.table_name,
            BackupName=f'{self.table_name}-backup'
        )
        print(f"Created on-demand backup of {self.table_name} in {self.env} as '{self.table_name}-backup'")
        return response

    def restore_from_backup(self, other) -> None:
        """Restores a table(self) from the backup(other) table.

        Truncates the original table, and copies the back up table to the original table.
        The backup table is then deleted.

        Args:
            self: the table to be restored.
            other: the backup table.
        Returns:
            None
        """
        self._truncate()
        other._copy_in_parallel_batch(self)
        other._get_dynamodb_client().delete_table(TableName=other.table_name)
        print(f'All items from {other.table_name} have been copied to {self.table_name}. {other.table_name} is now deleted')

    def _truncate(self) -> None:
        """Iterates over the scans and delete all items with a batch writer.
        
        Returns:
            None
        """
        session = boto3.Session(profile_name=self.env)
        resource = session.resource('dynamodb')
        table = resource.Table(self.table_name)

        #get the table keys
        table_keys = [key.get("AttributeName") for key in table.key_schema]
        #Only retrieve the keys for each item in the table (minimize data transfer)
        projectionExpression = ", ".join('#' + key for key in table_keys)
        expressionAttrNames = {'#'+key: key for key in table_keys}
        
        counter = 0
        page = table.scan(ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames)
        with table.batch_writer() as batch:
            while page["Count"] > 0:
                counter += page["Count"]
                # Delete items in batches
                for itemKeys in page["Items"]:
                    batch.delete_item(Key=itemKeys)
                # Fetch the next page
                if 'LastEvaluatedKey' in page:
                    page = table.scan(
                        ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames,
                        ExclusiveStartKey=page['LastEvaluatedKey'])
                else:
                    break
        print(f"Truncated all {counter} items from {self.table_name} in {self.env}.")

    def _copy_in_parallel_batch(self, other) -> None:
        """Scans all items in source(self) table and puts items to target(other) table by segments in parallel.
        
        Args:
            self: source table.
            other: target table.
        Returns:
            None
        """
        def scan(env: str, segment: int, total_segments: int) -> List[Dict]:
            session = boto3.Session(profile_name=env)
            client = session.client('dynamodb')

            paginator = client.get_paginator('scan')
            scan_iterator = paginator.paginate(
                TableName=self.table_name,
                Select='ALL_ATTRIBUTES',
                ReturnConsumedCapacity='NONE',
                ConsistentRead=True,
                Segment=segment,
                TotalSegments=total_segments
            )
            items = []
            for page in scan_iterator:
                items.extend(page['Items'])

            print(f'Scanned {len(items)} items in parallel')
            return items

        num_threads = os.cpu_count()
        pool = mp.Pool(num_threads)

        put_batch_items_with_retry = other._write_batch_items_with_retry(env=other.env, operation_request='PutRequest')
        counter = 0
        for segment in range(num_threads):
            result = pool.apply_async(scan, args=(self.env, segment, num_threads))
            items = result.get()
            counter += len(items)

            batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
            pool.starmap(put_batch_items_with_retry, batches)
            print(f"Put {len(items)} to {other.table_name} in {other.env}")

        pool.close()
        pool.join()
        print(f"Copied {counter} items from {self.table_name} in {self.env} to {other.table_name} in {other.env}")

    def _copy_items_in_parallel_batch(self, other, source_items, target_items):
        """Deletes target items from the target table and put source items in the table.
        
        Args:
            self: source table.
            other: target table.
            source_items: items queried in the source table to put.
            target_items: items queried in the target table to delete.
        """
        # Delete target items from the target table and put source items in parallel batch operations
        delete_batch_items_with_retry = other._write_batch_items_with_retry(env=other.env, operation_request='DeleteRequest')
        put_batch_items_with_retry = other._write_batch_items_with_retry(env=other.env, operation_request='PutRequest')

        # Chunk items into batches of size = BATCH_SIZE
        source_batches = [source_items[i:i + BATCH_SIZE] for i in range(0, len(source_items), BATCH_SIZE)]
        target_batches = [target_items[i:i + BATCH_SIZE] for i in range(0, len(target_items), BATCH_SIZE)]

        with mp.Pool(os.cpu_count()) as pool:
            pool.starmap(delete_batch_items_with_retry, target_batches)
            print(f"Deleted {len(target_items)} items from {other.table_name} in {other.env}")
            pool.starmap(put_batch_items_with_retry, source_batches)
            print(f"Put {len(source_items)} items to {other.table_name} in {other.env}")
            pool.close()
            pool.join()
            
        print(f"Copied {len(source_items)} items from {self.table_name} in {self.env} to {other.table_name} in {other.env}")

    def _write_batch_items_with_retry(self, env: str, operation_request: str):
        """Returns a __write_batch_items_with_retry function with a given operation request."""
        def __write_batch_items_with_retry(*args) -> None:
            """Writes items in batch and retries any unprocessed items with an exponential backoff.

            Args:
                *args: list of items. Size of the list must be less than 25.
            Returns:
                None
            Raises:
                Exception: put_item returned a problem.
            """
            session = boto3.Session(profile_name=env)
            client = session.client('dynamodb')
            
            response = self._write_batch_items(client, operation_request, args)
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception(f"{operation_request} returned a problem writing batch items: {response}.")
            backoff_time = 3
            while response['UnprocessedItems']:
                print(f"Unprocessed items detected... doing backoff and trying again...{backoff_time}")
                time.sleep(backoff_time)
                backoff_time *= 2 #double it for each iteration
                response = self._write_batch_unprocessed_items(client, response)
                if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise Exception(f"{operation_request} returned a problem writing batch unprocessed items: {response}")

        return __write_batch_items_with_retry

    def _write_batch_items(self, client, operation_request: str, items: List[Dict]) -> Dict:
        """Writes items in batch and returns the response."""
        if operation_request == 'PutRequest':
            request_items=[{operation_request: {'Item': item}} for item in items]
        elif operation_request == 'DeleteRequest':
            request_items=[{operation_request: {'Key':{self.pk_name: item[self.pk_name], self.sk_name: item[self.sk_name]}}} for item in items]
        else:
            raise Exception(f'operation_request must be either "Put Request" or "Delete Request". operation_requuest = {operation_request}')

        response = client.batch_write_item(
        RequestItems={
                self.table_name : request_items
            },
            ReturnConsumedCapacity='TOTAL',
            ReturnItemCollectionMetrics='NONE'
        )
        return response

    def _write_batch_unprocessed_items(self, client, response_with_unprocessed: Dict) -> Dict:
        """Writes unprocssed items in batch and returns the response."""
        response = client.batch_write_item(
        RequestItems=response_with_unprocessed['UnprocessedItems'],
            ReturnConsumedCapacity='TOTAL',
            ReturnItemCollectionMetrics='NONE'
        )
        return response

    def query_with_filter(self, pk: str, attr_key: str, attr_value: str, index_name: str = None) -> List[str]:
        """Queries with filter expression.
        
        Args:
            pk: partition key of the table or of a secondary index if index_name is specified.
            attr_key: Attribute key in filter expression.
            attr_value: Attribute value in filter expression.
            index_name: Optional; name of the secondary index to query in.
        Returns:
            List of queried items.
        """

        session = boto3.Session(profile_name=self.env)
        resource = session.resource('dynamodb')
        table = resource.Table(self.table_name)

        if index_name:
            response = table.query(
                TableName=self.table_name,
                IndexName = index_name,
                KeyConditionExpression = Key(self.pk_name).eq(pk),
                FilterExpression = Attr(attr_key).begins_with(attr_value)
            )
        
        else:
            pk_name, _ = self._get_secondary_key(index_name)
            response = table.query(
                TableName=self.table_name,
                KeyConditionExpression = Key(pk_name).eq(pk),
                FilterExpression = Attr(attr_key).begins_with(attr_value)
            )
        
        return response['Items']

    def _get_dynamodb_client(self):
        session = boto3.Session(profile_name=self.env)
        client = session.client('dynamodb')

        return client

    def _get_primary_key(self) -> tuple:
        """Returns the names of primary partition and sort key as a tuple."""
        response = self._get_dynamodb_client().describe_table(
            TableName=self.table_name
        )

        pk_name = None
        sk_name = None
        keys = response['Table']['KeySchema']
        for key in keys:
            if key['KeyType'] == 'HASH':
                pk_name = key['AttributeName']
            elif key['KeyType'] == 'RANGE':
                sk_name = key['AttributeName']

        return pk_name, sk_name

    def _get_secondary_key(self, index_name: str) -> tuple:
        """Returns the names of secondary partition and sort key of a secondary index as a tuple."""
        response = self._get_dynamodb_client().describe_table(
            TableName=self.table_name
        )

        pk_name = None
        sk_name = None
        keys = [index['KeySchema'] for index in response['Table']['GlobalSecondaryIndexes'] if index['IndexName'] == index_name][0]
        for key in keys:
            if key['KeyType'] == 'HASH':
                pk_name = key['AttributeName']
            elif key['KeyType'] == 'RANGE':
                sk_name = key['AttributeName']
        
        return pk_name, sk_name

    def _table_arn(self):
        """Returns the TableArn."""
        response = self._get_dynamodb_client().describe_table(TableName=self.table_name)
        return response['Table']['TableArn']


