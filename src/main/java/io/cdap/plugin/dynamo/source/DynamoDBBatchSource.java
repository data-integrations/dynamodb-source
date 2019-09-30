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
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
  private final DynamoDBBatchSourceConfig config;
  private Schema schema;

  public DynamoDBBatchSource(DynamoDBBatchSourceConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    schema = config.getParsedSchema();
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();

    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    stageConfigurer.setOutputSchema(config.getParsedSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    context.setInput(Input.of(config.referenceName, new DynamoDBInputFormatProvider(config)));
  }

  @Override
  public void transform(KeyValue<LongWritable, Item> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder;

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
   * Input format provider for DynamoDBBatchSource.
   */
  private static class DynamoDBInputFormatProvider implements InputFormatProvider {
    private Map<String, String> conf;

    DynamoDBInputFormatProvider(DynamoDBBatchSourceConfig dynamoDBConfig) {
      this.conf = new HashMap<>();
      conf.put(DynamoDBConstants.ACCESS_KEY, dynamoDBConfig.getAccessKey());
      conf.put(DynamoDBConstants.SECRET_ACCESS_KEY, dynamoDBConfig.getSecretAccessKey());
      conf.put(DynamoDBConstants.REGION, dynamoDBConfig.getRegion() == null ? "" : dynamoDBConfig.getRegion());
      conf.put(DynamoDBConstants.ENDPOINT_URL, dynamoDBConfig.getEndpointUrl() == null ? "" :
        dynamoDBConfig.getEndpointUrl());
      conf.put(DynamoDBConstants.TABLE_NAME, dynamoDBConfig.getTableName());
      conf.put(DynamoDBConstants.QUERY, dynamoDBConfig.getQuery());
      conf.put(DynamoDBConstants.FILTER_QUERY, dynamoDBConfig.getFilterQuery() == null ? "" :
        dynamoDBConfig.getFilterQuery());
      conf.put(DynamoDBConstants.NAME_MAPPINGS, dynamoDBConfig.getNameMappings() == null ? "" :
        dynamoDBConfig.getNameMappings());
      conf.put(DynamoDBConstants.VALUE_MAPPINGS, dynamoDBConfig.getValueMappings());
      conf.put(DynamoDBConstants.PLACEHOLDERS_TYPE, dynamoDBConfig.getPlaceholderType());
      conf.put(DynamoDBConstants.READ_THROUGHPUT, dynamoDBConfig.getReadThroughput() == null ?
        DynamoDBConstants.DEFAULT_READ_THROUGHPUT : dynamoDBConfig.getReadThroughput());
      conf.put(DynamoDBConstants.READ_THROUGHPUT_PERCENT, dynamoDBConfig.getReadThroughputPercentage() == null ?
        DynamoDBConstants.DEFAULT_THROUGHPUT_PERCENTAGE : dynamoDBConfig.getReadThroughputPercentage());
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
