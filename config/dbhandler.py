import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")

class DynamoDBHandler:
    def __init__(self, table_name):
        # initializing table name and table
        self.table_name = table_name
        self.table = dynamodb.Table(table_name)
    
    def get_partition_key(self):
        # getting partition key dynamically
        table_desc = self.table.meta.client.describe_table(TableName=self.table.name)
        key_schema = table_desc["Table"]["KeySchema"]
        partition_key = next(item["AttributeName"] for item in key_schema if item["KeyType"] == "HASH")
        return partition_key

    def get_item(self,key):
        # getting item on partition key
        response = self.table.get_item(
            Key={
                    self.get_partition_key(): key,# Partition key
                }
            )

        if "Item" in response:
            print("✅ Retrieved item:", response["Item"])
            return response["Item"]
        else:
            print("❌ No item found with those keys")
            return None

    def fetch_all_items(self):
        # fetching all items from the table
        response = self.table.scan()
        return response['Items']

    def fetch_items_on_field(self, field,field_value):
        # fething field on the field
        response = self.table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr(field).eq(field_value)
        )
        return response.get("Items", [])

    def update_all_group_items(self, filter_field,filter_field_value, update_field, update_field_value,expected_field, expected_field_value):
        # update group of items with mentioned fields
        response = self.table.scan(
            FilterExpression=Attr(filter_field).eq(filter_field_value)
        )
        items = response.get("Items", [])
        pending_items = [i for i in items if i.get(expected_field) == expected_field_value]
        #Update each item by its primary key
        for item in pending_items:
            print(item, "pending item")
            self.table.update_item(
                Key={self.get_partition_key(): item["id"]},   # id is the partition key
                UpdateExpression="SET #s = :newStatus",
                ExpressionAttributeNames={"#s": update_field},
                ConditionExpression="#s = :expectedStatus",
                ExpressionAttributeValues={":newStatus": update_field_value,":expectedStatus": expected_field_value}
            )
            {'grp_count': Decimal('7'), 'sgrp_name': 'SG3', 'grp_actual_count': Decimal('7'), 'sgrp_pattern': '^((JSC)\\.(ELECPAPR)\\.(\\d{8})\\.(\\d{6})\\.(zip|ZIP))$', 'grp_completion_status': 'completed', 'wait_time': Decimal('180'), 'sgrp_id': Decimal('3'), 'grp_complete_action': {'action_type': ['file_merger']}, 'sgrp_count': Decimal('2'), 'grp_id': Decimal('1'), 'grp_merge_status': 'pending', 'grp_name': 'G1', 'output_prefix': 'janus/taxes/merge', 'grp_merge_path': 'janus/taxes/merge', 'id': Decimal('16'), 'file_name': 'JSC.ELECPAPR.20250817.155727.zip', 'object_key': 'janus/taxes/monitoring/', 'grp_run_id': 'JSC.YETAX.20250817.G1', 'product': 'taxes', 'grp_incomplete_action': {'action_type': ['email']}, 'grp_status': 'pending', 'grp_initial_status': 'pending', 'bucket': 'br-icsdev-dpmjhistmt-dataingress-us-east-1-s3', 'sgrp_status': 'pending', 'sgrp_run_id': 'JSC.YETAX.20250817.G1.SG3', 'client': 'janus', 'sgrp_actual_count': Decimal('2'), 'content_merge_status': 'pending'}
    def insert_item(self, item):
        # inserting records into the table
        try:
            self.table.put_item(Item=item)
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            print(f"⚠️ id already exists — skipping insert")
            raise ValueError("id already exists")