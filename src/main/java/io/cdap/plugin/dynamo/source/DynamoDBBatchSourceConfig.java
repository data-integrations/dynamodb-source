/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Config class for DynamoDBBatchSource.
 */
public class DynamoDBBatchSourceConfig extends ReferencePluginConfig {
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_ACCESS_KEY = "secretAccessKey";
  public static final String REGION = "region";
  public static final String ENDPOINT_URL = "endpointUrl";
  public static final String TABLE_NAME = "tableName";
  public static final String QUERY = "query";
  public static final String FILTER_QUERY = "filterQuery";
  public static final String NAME_MAPPINGS = "nameMappings";
  public static final String VALUE_MAPPINGS = "valueMappings";
  public static final String PLACEHOLDER_TYPE = "placeholderType";
  public static final String READ_THROUGHPUT = "readThroughput";
  public static final String READ_THROUGHPUT_PERCENTAGE = "readThroughputPercentage";
  public static final String SCHEMA = "schema";

  @Name(ACCESS_KEY)
  @Description("Access key for AWS DynamoDB to connect to. (Macro Enabled)")
  @Macro
  private String accessKey;

  @Name(SECRET_ACCESS_KEY)
  @Description("Secret access key for AWS DynamoDB to connect to. (Macro Enabled)")
  @Macro
  private String secretAccessKey;

  @Name(REGION)
  @Description("The region for AWS DynamoDB to connect to. Default is us-west-2 i.e. US West (Oregon).")
  @Nullable
  private String region;

  @Name(ENDPOINT_URL)
  @Description("The endpoint URL for AWS DynamoDB instance to connect to. For example, dynamodb.us-east-1.amazonaws" +
    ".com. If not provided, then the 'Endpoint URL' will be constructed using 'Region'.")
  @Nullable
  private String endpointUrl;

  @Name(TABLE_NAME)
  @Description("The DynamoDB table to read the data from.")
  private String tableName;

  @Name(QUERY)
  @Description("Query to read the items from DynamoDB table. Query must include a partition key value and an " +
    "equality condition and it must be specified in the following format: 'hashAttributeName = :hashval'. For " +
    "example, ID = :v_id or ID = :v_id AND Title = :v_title, if sort key condition is used to read the data from " +
    "table. (Macro Enabled)")
  @Macro
  private String query;

  @Name(FILTER_QUERY)
  @Description("Filter query to return only the desired items that satisfy the condition. All other items are " +
    "discarded. It must be specified in the similar format like main query. For example, rating = :v_rating. " +
    "(Macro Enabled)")
  @Nullable
  @Macro
  private String filterQuery;

  @Name(NAME_MAPPINGS)
  @Description("List of the placeholder tokens used as the attribute name in the 'Query or FilterQuery' along with " +
    "their actual attribute names. This is a comma-separated list of key-value pairs, where each pair is separated " +
    "by a pipe sign '|' and specifies the tokens and actual attribute names. For example, '#yr|year', if the query " +
    "is like: '#yr = :yyyy'. This might be necessary if an attribute name conflicts with a DynamoDB reserved word. " +
    "(Macro Enabled)")
  @Nullable
  @Macro
  private String nameMappings;

  @Name(VALUE_MAPPINGS)
  @Description("List of the placeholder tokens used as the attribute values in the 'Query or FilterQuery' along " +
    "with their actual values. This is a comma-separated list of key-value pairs, where each pair is separated by a" +
    " pipe sign '|' and specifies the tokens and actual values. For example, ':v_id|256,:v_title|B', if the query " +
    "is like: 'ID = :v_id AND Title = :v_title'. (Macro Enabled)")
  @Macro
  private String valueMappings;

  @Name(PLACEHOLDER_TYPE)
  @Description("List of the placeholder tokens used as the attribute values in the 'Query or FilterQuery' along " +
    "with their data types. This is a comma-separated list of key-value pairs, where each pair is separated by a " +
    "pipe sign '|' and specifies the tokens and its type. For example, ':v_id|int,:v_title|string', if the query is" +
    " like: 'ID = :v_id AND Title = :v_title'. Supported types are: 'boolean, int, long, float, double and string'." +
    " (Macro Enabled)")
  @Macro
  private String placeholderType;

  @Name(READ_THROUGHPUT)
  @Description("Read Throughput for AWS DynamoDB table to connect to, in double. Default is 1. (Macro Enabled)")
  @Nullable
  @Macro
  private String readThroughput;

  @Name(READ_THROUGHPUT_PERCENTAGE)
  @Description("Read Throughput Percentage for AWS DynamoDB table to connect to. Default is 0.5. (Macro Enabled)")
  @Nullable
  @Macro
  private String readThroughputPercentage;

  @Name(SCHEMA)
  @Description("Specifies the schema that has to be output. If not specified, then by default each item will be " +
    "emitted as a JSON string, in the 'body' field of the StructuredRecord.")
  private String schema;

  public DynamoDBBatchSourceConfig(String referenceName, String accessKey, String secretAccessKey,
                                   @Nullable String region, @Nullable String endpointUrl, String tableName,
                                   String query, @Nullable String filterQuery, @Nullable String nameMappings,
                                   String valueMappings, String placeholderType, @Nullable String readThroughput,
                                   @Nullable String readThroughputPercentage, String schema) {
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

  private DynamoDBBatchSourceConfig(Builder builder) {
    super(builder.referenceName);
    accessKey = builder.accessKey;
    secretAccessKey = builder.secretAccessKey;
    region = builder.region;
    endpointUrl = builder.endpointUrl;
    tableName = builder.tableName;
    query = builder.query;
    filterQuery = builder.filterQuery;
    nameMappings = builder.nameMappings;
    valueMappings = builder.valueMappings;
    placeholderType = builder.placeholderType;
    readThroughput = builder.readThroughput;
    readThroughputPercentage = builder.readThroughputPercentage;
    schema = builder.schema;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(DynamoDBBatchSourceConfig copy) {
    return builder()
      .setReferenceName(copy.referenceName)
      .setAccessKey(copy.getAccessKey())
      .setSecretAccessKey(copy.getSecretAccessKey())
      .setRegion(copy.getRegion())
      .setEndpointUrl(copy.getEndpointUrl())
      .setTableName(copy.getTableName())
      .setQuery(copy.getQuery())
      .setFilterQuery(copy.getFilterQuery())
      .setNameMappings(copy.getNameMappings())
      .setValueMappings(copy.getValueMappings())
      .setPlaceholderType(copy.getPlaceholderType())
      .setReadThroughput(copy.getReadThroughput())
      .setReadThroughputPercentage(copy.getReadThroughputPercentage())
      .setSchema(copy.getSchema());
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  @Nullable
  public String getRegion() {
    return region;
  }

  @Nullable
  public String getEndpointUrl() {
    return endpointUrl;
  }

  public String getTableName() {
    return tableName;
  }

  public String getQuery() {
    return query;
  }

  @Nullable
  public String getFilterQuery() {
    return filterQuery;
  }

  @Nullable
  public String getNameMappings() {
    return nameMappings;
  }

  public String getValueMappings() {
    return valueMappings;
  }

  public String getPlaceholderType() {
    return placeholderType;
  }

  @Nullable
  public String getReadThroughput() {
    return readThroughput;
  }

  @Nullable
  public String getReadThroughputPercentage() {
    return readThroughputPercentage;
  }

  public String getSchema() {
    return schema;
  }

  public void validate(FailureCollector failureCollector) {
    IdUtils.validateReferenceName(referenceName, failureCollector);

    if (!containsMacro(ACCESS_KEY) && Strings.isNullOrEmpty(accessKey)) {
      failureCollector.addFailure("Access key must be specified.", null)
        .withConfigProperty(ACCESS_KEY);
    }

    if (!containsMacro(SECRET_ACCESS_KEY) && Strings.isNullOrEmpty(secretAccessKey)) {
      failureCollector.addFailure("Secret access key must be specified.", null)
        .withConfigProperty(SECRET_ACCESS_KEY);
    }

    validateTableName(failureCollector);

    if (!containsMacro(QUERY) && Strings.isNullOrEmpty(query)) {
      failureCollector.addFailure("Query must be specified.", null)
        .withConfigProperty(QUERY);
    }

    if (!containsMacro(VALUE_MAPPINGS) && Strings.isNullOrEmpty(valueMappings)) {
      failureCollector.addFailure("Value mappings must be specified.", null)
        .withConfigProperty(VALUE_MAPPINGS);
    }

    if (!containsMacro(PLACEHOLDER_TYPE) && Strings.isNullOrEmpty(placeholderType)) {
      failureCollector.addFailure("Placeholder type must be specified.", null)
        .withConfigProperty(PLACEHOLDER_TYPE);
    }

    validateSchema(failureCollector);
  }

  public Schema getParsedSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalStateException("Invalid schema: " + e.getMessage(), e);
    }
  }

  /**
   * Validates whether the table name follows the DynamoDB naming rules and conventions or not.
   */
  private void validateTableName(FailureCollector failureCollector) {
    if (Strings.isNullOrEmpty(tableName)) {
      failureCollector.addFailure("Table name must be specified.", null)
        .withConfigProperty(TABLE_NAME);
    } else {
      int tableNameLength = tableName.length();
      if (tableNameLength < 3 || tableNameLength > 255) {
        failureCollector.addFailure(
          String.format("Table name '%s' does not follow the DynamoDB naming rules.", tableName),
          "Table name must be between 3 and 255 characters long.")
          .withConfigProperty(TABLE_NAME);
      }

      String pattern = "^[a-zA-Z0-9_.-]+$";
      Pattern patternObj = Pattern.compile(pattern);
      Matcher matcher = patternObj.matcher(tableName);

      if (!matcher.find()) {
        failureCollector.addFailure(
          String.format("Table name '%s' does not follow the DynamoDB naming rules.", tableName),
          "Table names can contain only the following characters: 'a-z, A-Z, 0-9, underscore(_), " +
            "dot(.) and dash(-)'.")
          .withConfigProperty(TABLE_NAME);

      }
    }
  }

  private void validateSchema(FailureCollector failureCollector) {
    if (Strings.isNullOrEmpty(schema)) {
      failureCollector.addFailure("Schema must be specified.", null)
        .withConfigProperty(SCHEMA);
    } else {
      try {
        Schema.parseJson(schema);
      } catch (IOException e) {
        failureCollector.addFailure("Invalid output schema: " + e.getMessage(), null)
          .withConfigProperty(SCHEMA)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  /**
   * Builder for creating a {@link DynamoDBBatchSourceConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String accessKey;
    private String secretAccessKey;
    private String region;
    private String endpointUrl;
    private String tableName;
    private String query;
    private String filterQuery;
    private String nameMappings;
    private String valueMappings;
    private String placeholderType;
    private String readThroughput;
    private String readThroughputPercentage;
    private String schema;

    private Builder() {
    }

    public Builder setReferenceName(String val) {
      referenceName = val;
      return this;
    }

    public Builder setAccessKey(String val) {
      accessKey = val;
      return this;
    }

    public Builder setSecretAccessKey(String val) {
      secretAccessKey = val;
      return this;
    }

    public Builder setRegion(String val) {
      region = val;
      return this;
    }

    public Builder setEndpointUrl(String val) {
      endpointUrl = val;
      return this;
    }

    public Builder setTableName(String val) {
      tableName = val;
      return this;
    }

    public Builder setQuery(String val) {
      query = val;
      return this;
    }

    public Builder setFilterQuery(String val) {
      filterQuery = val;
      return this;
    }

    public Builder setNameMappings(String val) {
      nameMappings = val;
      return this;
    }

    public Builder setValueMappings(String val) {
      valueMappings = val;
      return this;
    }

    public Builder setPlaceholderType(String val) {
      placeholderType = val;
      return this;
    }

    public Builder setReadThroughput(String val) {
      readThroughput = val;
      return this;
    }

    public Builder setReadThroughputPercentage(String val) {
      readThroughputPercentage = val;
      return this;
    }

    public Builder setSchema(String val) {
      schema = val;
      return this;
    }

    public DynamoDBBatchSourceConfig build() {
      return new DynamoDBBatchSourceConfig(this);
    }
  }
}
