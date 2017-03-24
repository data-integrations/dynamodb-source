# DynamoDB Batch Source


Description
-----------
DynamoDB Batch Source that will read the data items from AWS DynamoDB table and convert each item into the
StructuredRecord as per the schema specified by the user, that can be further processed downstream in the pipeline.
User can provide the query, to read the items from DynamoDB table.

Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**accessKey:** Access key for AWS DynamoDB to connect to. (Macro Enabled)

**secretAccessKey:** Secret access key for AWS DynamoDB to connect to. (Macro Enabled)

**region:** The region for AWS DynamoDB to connect to. Default is us-west-2 i.e. US West (Oregon).

**endpointUrl:** The endpoint URL for AWS DynamoDB instance to connect to. For example, dynamodb.us-east-1.amazonaws
.com. If not provided, then the 'Endpoint URL' will be constructed using 'Region'.

**tableName:** The DynamoDB table to read the data from.

**query:** Query to read the items from DynamoDB table. Query must include a partition key value and an equality
condition and it must be specified in the following format: 'hashAttributeName = :hashval'. For example, ID = :v_id or
ID = :v_id AND Title = :v_title, if sort key condition is used to read the data from table. (Macro Enabled)

**filterQuery:** Filter query to return only the desired items that satisfy the condition. All other items are
discarded. It must be specified in the similar format like main query. For example, rating = :v_rating. (Macro Enabled)

**nameMappings:** List of the placeholder tokens used as the attribute name in the 'Query or FilterQuery' along with
their actual attribute names. This is a comma-separated list of key-value pairs, where each pair is separated by a
pipe sign '|' and specifies the tokens and actual attribute names. For example, '#yr|year', if the query is like:
'#yr = :yyyy'. This might be necessary if an attribute name conflicts with a DynamoDB reserved word. (Macro Enabled)

**valueMappings:** List of the placeholder tokens used as the attribute values in the 'Query or FilterQuery' along with
their actual values. This is a comma-separated list of key-value pairs, where each pair is separated by a pipe sign '|'
and specifies the tokens and actual values. For example, ':v_id|256,:v_title|B', if the query is like: 'ID = :v_id AND
Title = :v_title'. (Macro Enabled)

**placeholderType:** List of the placeholder tokens used as the attribute values in the 'Query or FilterQuery' along
with their data types. This is a comma-separated list of key-value pairs, where each pair is separated by a pipe sign
'|' and specifies the tokens and its type. For example, ':v_id|int,:v_title|string', if the query is like:
'ID = :v_id AND Title = :v_title'. Supported types are: 'boolean, int, long, float, double and string'. (Macro Enabled)

**readThroughput:** Read Throughput for AWS DynamoDB table to connect to, in double. Default is 1. (Macro Enabled)

**readThroughputPercentage:** Read Throughput Percentage for AWS DynamoDB table to connect to. Default is 0.5.
(Macro Enabled)

**schema:** Specifies the schema that has to be output. If not specified, then by default each item will be emitted
as a JSON string, in the 'body' field of the StructuredRecord.

Conditions
----------
Any invalid configuration('accessKey' or 'secretAccessKey' or 'region' or 'endpointUrl') for connecting to AWS DynamoDB
instance, will result into the runtime failure.

If the user provides both 'region' and 'endpointUrl', then the plugin will connect to the AWS DynamoDB instance,
pointed by the 'endpointUrl'.

Table must exists in the DynamoDB, before reading the items. If not, then it will result into the runtime failure.

Query must follow the DynamoDB rules and supported format. Any mismatch in the query will result into the runtime
failure.

If user does not provide the output schema, then by default each item will be emitted as a JSON string, in the 'body'
 field of the StructuredRecord.

Example
-------
This example connects to a DynamoDB instance using the 'accessKey, secretAccessKey and regionID', and reads the data
from table 'UserInfoTable'. It will read the items with partition key 'ID' as 120.

    {
      "name": "DynamoDB",
      "type": "batchsource",
        "properties": {
          "accessKey": "testAccessKey",
          "secretAccessKey": "testSecretKey",
          "region": "us-east-1",
          "tableName": "UserInfoTable",
          "query": "ID = :v_id",
          "valueMappings": ":v_id|120"
          "placeholderType": ":v_id|int"
        }
    }

For example, the DynamoDB source will read the items from table, stored the JSON output in 'body' field and emits the
 Structured Record to the next stage:

    +=====================================+
    | body : STRING                       |
    +=====================================+
    | {                                   |
    |   "PageCount" : 500,                |
    |   "Price" : 60,                     |
    |   "Id" : 120,                       |
    |   "Title" : "Book 120 Title Part-I" |
    | }                                   |
    +=====================================+
    | {                                   |
    |   "PageCount" : 900,                |
    |   "Price" : 90,                     |
    |   "Id" : 120,                       |
    |   "Title" : "Book 120 Title Part-II"|
    | }                                   |
    +=====================================+
