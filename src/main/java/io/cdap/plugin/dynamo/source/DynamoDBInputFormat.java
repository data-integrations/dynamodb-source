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

import io.cdap.plugin.dynamo.source.split.DynamoDBSplitGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * Input Format class for DynamoDBBatchSource.
 */
public class DynamoDBInputFormat extends InputFormat {

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();
    int maxClusterMapTasks = conf.getInt("mapreduce.tasktracker.map.tasks.maximum", 1);
    if (maxClusterMapTasks < 1) {
      throw new RuntimeException("Number of map tasks configured for the cluster is less than 1. Map tasks: " +
                                   maxClusterMapTasks);
    }

    double readPercentage = Double.parseDouble(conf.get(DynamoDBConstants.READ_THROUGHPUT_PERCENT));
    if (readPercentage <= 0) {
      throw new RuntimeException("Invalid read percentage: " + readPercentage);
    }

    double maxReadThroughputAllocated = Double.parseDouble(conf.get(DynamoDBConstants.READ_THROUGHPUT));
    if (maxReadThroughputAllocated < 1.0) {
      throw new RuntimeException("Read throughput should not be less than 1. Read throughput percent: " +
                                   maxReadThroughputAllocated);
    }

    int configuredReadThroughput = (int) Math.floor(maxReadThroughputAllocated * readPercentage);
    if (configuredReadThroughput < 1) {
      configuredReadThroughput = 1;
    }

    int numSegments = getNumSegments((int) maxReadThroughputAllocated, conf);
    int numMappers = getNumMappers(maxClusterMapTasks, configuredReadThroughput, conf);
    return getSplitGenerator().generateSplits(numMappers, numSegments, conf);
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws
    IOException, InterruptedException {
    return new DynamoDBRecordReader();
  }

  private int getNumSegments(int tableNormalizedReadThroughput, Configuration conf) throws IOException {
    // Segments for total throughput.
    int numSegments = (int) (tableNormalizedReadThroughput / DynamoDBConstants.MIN_IO_PER_SEGMENT);
    // Fit to bounds.
    numSegments = Math.min(numSegments, DynamoDBConstants.MAX_SCAN_SEGMENTS);
    numSegments = Math.max(numSegments, DynamoDBConstants.MIN_SCAN_SEGMENTS);
    return numSegments;
  }

  private int getNumMappers(int maxClusterMapTasks, int configuredReadThroughput, Configuration conf)
    throws IOException {
    int numMappers = maxClusterMapTasks;
    int maxMapTasksForThroughput = configuredReadThroughput / DynamoDBConstants.MIN_READ_THROUGHPUT_PER_MAP;
    if (maxMapTasksForThroughput < maxClusterMapTasks) {
      numMappers = maxMapTasksForThroughput;
    }
    // Don't need more mappers than max possible scan segments.
    int maxSplits = Math.min(DynamoDBConstants.MAX_SCAN_SEGMENTS, conf.getInt(DynamoDBConstants.MAX_MAP_TASKS,
                                                                              DynamoDBConstants.MAX_SCAN_SEGMENTS));

    if (numMappers > maxSplits) {
      numMappers = maxSplits;
    }
    numMappers = Math.max(numMappers, DynamoDBConstants.MIN_SCAN_SEGMENTS);
    return numMappers;
  }

  private DynamoDBSplitGenerator getSplitGenerator() {
    return new DynamoDBSplitGenerator();
  }
}
