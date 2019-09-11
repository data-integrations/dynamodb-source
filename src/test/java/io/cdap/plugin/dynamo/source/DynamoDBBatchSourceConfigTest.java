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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Unit Tests for DynamoDBBatchSourceConfig.
 */
public class DynamoDBBatchSourceConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema SCHEMA = Schema.recordOf(
    "schema", Schema.Field.of("ID", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
  private static final DynamoDBBatchSourceConfig VALID_CONFIG = new DynamoDBBatchSourceConfig(
    "Referencename",
    "testKey",
    "testKey",
    "us-east-1",
    "",
    "table",
    "Id = :v_iD",
    "",
    "",
    ":v_iD|120",
    ":v_iD|int",
    "",
    "",
    SCHEMA.toString()
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector("mockStage");
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidTableNameLength() {
    DynamoDBBatchSourceConfig config = DynamoDBBatchSourceConfig.builder(VALID_CONFIG)
      .setTableName("tm")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, DynamoDBBatchSourceConfig.TABLE_NAME);
  }

  @Test
  public void testInvalidTableName() {
    DynamoDBBatchSourceConfig config = DynamoDBBatchSourceConfig.builder(VALID_CONFIG)
      .setTableName("wrong%^table")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, DynamoDBBatchSourceConfig.TABLE_NAME);
  }

  @Test
  public void testEmptySchema() {
    DynamoDBBatchSourceConfig config = DynamoDBBatchSourceConfig.builder(VALID_CONFIG)
      .setSchema("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, DynamoDBBatchSourceConfig.SCHEMA);
  }

  @Test
  public void testInvalidSchema() {
    DynamoDBBatchSourceConfig config = DynamoDBBatchSourceConfig.builder(VALID_CONFIG)
      .setSchema("{[}")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, DynamoDBBatchSourceConfig.SCHEMA);
    assertValidationFailedWithStacktrace(failureCollector);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList());
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private static void assertValidationFailedWithStacktrace(MockFailureCollector failureCollector) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STACKTRACE) != null)
      .collect(Collectors.toList());
    Assert.assertEquals(1, causeList.size());
  }
}
