/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.dynamo.source;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Batch DynamoDB Source Plugin - Reads the data from AWS DynamoDB table.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("DynamoDB")
@Description("DynamoDB Batch Source that will read the data items from AWS DynamoDB table and convert each item into " +
  "the StructuredRecord as per the schema specified by the user, that can be further processed downstream in the " +
  "pipeline.")
public class DynamoDBBatchSource extends BatchSource<LongWritable, Item, StructuredRecord> {
  private static final Schema DEFAULT_OUTPUT_SCHEMA = Schema.recordOf(
    "output", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private final DynamoDBConfig config;

  public DynamoDBBatchSource(DynamoDBConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validateTableName();
    try {
      Schema schema = Schema.parseJson(config.schema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    context.setInput(Input.of(config.referenceName, new DynamoDBInputFormatProvider(config)));
  }

  @Override
  public void transform(KeyValue<LongWritable, Item> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder;
    Schema schema;
    try {
      schema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
    if (schema.equals(DEFAULT_OUTPUT_SCHEMA)) {
      builder = StructuredRecord.builder(schema);
      builder.set("body", input.getValue().toJSONPretty());
    } else {
      builder = StructuredRecord.builder(schema);
      for (Schema.Field field : schema.getFields()) {
        builder.set(field.getName(), extractValue(input.getValue(), field));
      }
    }
    emitter.emit(builder.build());
  }

  /**
   * Extracts and returns the value from item as per the type specified.
   *
   * @param item
   * @param field
   * @return
   * @throws Exception
   */
  private Object extractValue(Item item, Schema.Field field) throws Exception {
    switch (field.getSchema().getType()) {
      case NULL:
        return null;
      case BOOLEAN:
        return item.getBOOL(field.getName());
      case INT:
        return item.getInt(field.getName());
      case LONG:
        return item.getLong(field.getName());
      case FLOAT:
        return item.getFloat(field.getName());
      case DOUBLE:
        return item.getDouble(field.getName());
      case BYTES:
        return item.getBinary(field.getName());
      case STRING:
        return item.getString(field.getName());
    }
    throw new IOException(
      String.format("Unsupported schema type: '%s' for field: '%s'. Supported types are 'boolean, int, long, float," +
                      "double, binary and string'.", field.getSchema(), field.getName()));
  }

  /**
   * Config class for DynamoDBBatchSource.
   */
  public static class DynamoDBConfig extends ReferencePluginConfig {

    @Description("Access key for AWS DynamoDB to connect to. (Macro Enabled)")
    @Macro
    private String accessKey;

    @Description("Secret access key for AWS DynamoDB to connect to. (Macro Enabled)")
    @Macro
    private String secretAccessKey;

    @Description("The region for AWS DynamoDB to connect to. Default is us-west-2 i.e. US West (Oregon).")
    @Nullable
    private String region;

    @Description("The endpoint URL for AWS DynamoDB instance to connect to. For example, dynamodb.us-east-1.amazonaws" +
      ".com. If not provided, then the 'Endpoint URL' will be constructed using 'Region'.")
    @Nullable
    private String endpointUrl;

    @Description("The DynamoDB table to read the data from.")
    private String tableName;

    @Description("Query to read the items from DynamoDB table. Query must include a partition key value and an " +
      "equality condition and it must be specified in the following format: 'hashAttributeName = :hashval'. For " +
      "example, ID = :v_id or ID = :v_id AND Title = :v_title, if sort key condition is used to read the data from " +
      "table. (Macro Enabled)")
    @Macro
    private String query;

    @Description("Filter query to return only the desired items that satisfy the condition. All other items are " +
      "discarded. It must be specified in the similar format like main query. For example, rating = :v_rating. " +
      "(Macro Enabled)")
    @Nullable
    @Macro
    private String filterQuery;

    @Description("List of the placeholder tokens used as the attribute name in the 'Query or FilterQuery' along with " +
      "their actual attribute names. This is a comma-separated list of key-value pairs, where each pair is separated " +
      "by a pipe sign '|' and specifies the tokens and actual attribute names. For example, '#yr|year', if the query " +
      "is like: '#yr = :yyyy'. This might be necessary if an attribute name conflicts with a DynamoDB reserved word. " +
      "(Macro Enabled)")
    @Nullable
    @Macro
    private String nameMappings;

    @Description("List of the placeholder tokens used as the attribute values in the 'Query or FilterQuery' along " +
      "with their actual values. This is a comma-separated list of key-value pairs, where each pair is separated by a" +
      " pipe sign '|' and specifies the tokens and actual values. For example, ':v_id|256,:v_title|B', if the query " +
      "is like: 'ID = :v_id AND Title = :v_title'. (Macro Enabled)")
    @Macro
    private String valueMappings;

    @Description("List of the placeholder tokens used as the attribute values in the 'Query or FilterQuery' along " +
      "with their data types. This is a comma-separated list of key-value pairs, where each pair is separated by a " +
      "pipe sign '|' and specifies the tokens and its type. For example, ':v_id|int,:v_title|string', if the query is" +
      " like: 'ID = :v_id AND Title = :v_title'. Supported types are: 'boolean, int, long, float, double and string'." +
      " (Macro Enabled)")
    @Macro
    private String placeholderType;

    @Description("Read Throughput for AWS DynamoDB table to connect to, in double. Default is 1. (Macro Enabled)")
    @Nullable
    @Macro
    private String readThroughput;

    @Description("Read Throughput Percentage for AWS DynamoDB table to connect to. Default is 0.5. (Macro Enabled)")
    @Nullable
    @Macro
    private String readThroughputPercentage;

    @Description("Specifies the schema that has to be output. If not specified, then by default each item will be " +
      "emitted as a JSON string, in the 'body' field of the StructuredRecord.")
    private String schema;

    public DynamoDBConfig(String referenceName, String accessKey, String secretAccessKey, @Nullable String region,
                          @Nullable String endpointUrl, String tableName, String query, @Nullable String filterQuery,
                          @Nullable String nameMappings, String valueMappings, String placeholderType,
                          @Nullable String readThroughput, @Nullable String readThroughputPercentage, String schema) {
      super(referenceName);
      this.accessKey = accessKey;
      this.secretAccessKey = secretAccessKey;
      this.region = region;
      this.endpointUrl = endpointUrl;
      this.tableName = tableName;
      this.query = query;
      this.filterQuery = filterQuery;
      this.nameMappings = nameMappings;
      this.valueMappings = valueMappings;
      this.placeholderType = placeholderType;
      this.readThroughput = readThroughput;
      this.readThroughputPercentage = readThroughputPercentage;
      this.schema = schema;
    }

    /**
     * Validates whether the table name follows the DynamoDB naming rules and conventions or not.
     */
    private void validateTableName() {
      int tableNameLength = tableName.length();
      if (tableNameLength < 3 || tableNameLength > 255) {
        throw new IllegalArgumentException(
          String.format("Table name '%s' does not follow the DynamoDB naming rules. Table name must be between 3 and " +
                          "255 characters long.", tableName));

      }

      String pattern = "^[a-zA-Z0-9_.-]+$";
      Pattern patternObj = Pattern.compile(pattern);

      if (!Strings.isNullOrEmpty(tableName)) {
        Matcher matcher = patternObj.matcher(tableName);
        if (!matcher.find()) {
          throw new IllegalArgumentException(
            String.format("Table name '%s' does not follow the DynamoDB naming rules. Table names can contain only " +
                            "the following characters: 'a-z, A-Z, 0-9, underscore(_), dot(.) and dash(-)'.",
                          tableName));
        }
      }
    }
  }

  /**
   * Input format provider for DynamoDBBatchSource.
   */
  private static class DynamoDBInputFormatProvider implements InputFormatProvider {
    private Map<String, String> conf;

    DynamoDBInputFormatProvider(DynamoDBConfig dynamoDBConfig) {
      this.conf = new HashMap<>();
      conf.put(DynamoDBConstants.ACCESS_KEY, dynamoDBConfig.accessKey);
      conf.put(DynamoDBConstants.SECRET_ACCESS_KEY, dynamoDBConfig.secretAccessKey);
      conf.put(DynamoDBConstants.REGION, dynamoDBConfig.region == null ? "" : dynamoDBConfig.region);
      conf.put(DynamoDBConstants.ENDPOINT_URL, dynamoDBConfig.endpointUrl == null ? "" : dynamoDBConfig.endpointUrl);
      conf.put(DynamoDBConstants.TABLE_NAME, dynamoDBConfig.tableName);
      conf.put(DynamoDBConstants.QUERY, dynamoDBConfig.query);
      conf.put(DynamoDBConstants.FILTER_QUERY, dynamoDBConfig.filterQuery == null ? "" : dynamoDBConfig.filterQuery);
      conf.put(DynamoDBConstants.NAME_MAPPINGS, dynamoDBConfig.nameMappings == null ? "" : dynamoDBConfig.nameMappings);
      conf.put(DynamoDBConstants.VALUE_MAPPINGS, dynamoDBConfig.valueMappings);
      conf.put(DynamoDBConstants.PLACEHOLDERS_TYPE, dynamoDBConfig.placeholderType);
      conf.put(DynamoDBConstants.READ_THROUGHPUT, dynamoDBConfig.readThroughput == null ?
        DynamoDBConstants.DEFAULT_READ_THROUGHPUT : dynamoDBConfig.readThroughput);
      conf.put(DynamoDBConstants.READ_THROUGHPUT_PERCENT, dynamoDBConfig.readThroughputPercentage == null ?
        DynamoDBConstants.DEFAULT_THROUGHPUT_PERCENTAGE : dynamoDBConfig.readThroughputPercentage);
    }

    @Override
    public String getInputFormatClassName() {
      return DynamoDBInputFormat.class.getName();
    }

    @Override
    public Map<String, String> getInputFormatConfiguration() {
      return conf;
    }
  }
}
