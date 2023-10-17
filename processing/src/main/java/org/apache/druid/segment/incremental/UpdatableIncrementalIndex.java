/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.incremental;

import com.google.common.base.Supplier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class UpdatableIncrementalIndex extends IncrementalIndex
{
  private static final Logger LOGGER = new Logger(UpdatableIncrementalIndex.class);

  protected UpdatableIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd,
      boolean preserveExistingMetrics,
      boolean useMaxMemoryEstimates
  )
  {
    super(
        incrementalIndexSchema,
        deserializeComplexMetrics,
        concurrentEventAdd,
        preserveExistingMetrics,
        useMaxMemoryEstimates
    );
  }

  @Override
  public FactsHolder getFacts()
  {
    return null;
  }

  @Override
  public boolean canAppendRow()
  {
    return true;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return "No reason";
  }

  @Override
  protected void initAggs(
      AggregatorFactory[] metrics,
      Supplier<InputRow> rowSupplier,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd
  )
  {

  }

  @Override
  protected AddToFactsResult addToFacts(
      InputRow row,
      IncrementalIndexRow key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier,
      boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException
  {
    LOGGER.info(row + " " + key);
    return new AddToFactsResult(0, 0, new ArrayList<>());
  }

  @Override
  public int getLastRowIndex()
  {
    return 0;
  }

  @Override
  protected float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return 0;
  }

  @Override
  protected long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return 0;
  }

  @Override
  protected Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return null;
  }

  @Override
  protected double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    return 0;
  }

  @Override
  protected boolean isNull(int rowOffset, int aggOffset)
  {
    return false;
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(@Nullable List<PostAggregator> postAggs, boolean descending)
  {
    return Collections.emptyList();
  }

  public static class Builder extends AppendableIndexBuilder
  {
    @Override
    protected UpdatableIncrementalIndex buildInner()
    {
      return new UpdatableIncrementalIndex(
          Objects.requireNonNull(incrementalIndexSchema, "incrementIndexSchema is null"),
          deserializeComplexMetrics,
          concurrentEventAdd,
          preserveExistingMetrics,
          useMaxMemoryEstimates
      );
    }
  }

}
