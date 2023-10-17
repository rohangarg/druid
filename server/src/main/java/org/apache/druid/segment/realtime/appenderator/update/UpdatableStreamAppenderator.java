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

package org.apache.druid.segment.realtime.appenderator.update;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.compress.utils.Lists;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.CPUTimeMetricQueryRunner;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.MetricsEmittingQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.PerSegmentOptimizingQueryRunner;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ReferenceCountingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ArrayListSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.SegmentNotWritableException;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@SuppressWarnings("CheckReturnValue")
public class UpdatableStreamAppenderator implements Appenderator
{
  private final Logger LOGGER = new Logger(UpdatableStreamAppenderator.class);
  private final String id;
  private final DataSchema schema;
  private final AppenderatorConfig config;
  private final StorageConnector storageConnector;
  private final AtomicInteger counter;
  private final String dataSource;
  private final UpdateSpec updateConfig;
  private volatile FileLock basePersistDirLock = null;
  private volatile FileChannel basePersistDirLockChannel = null;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QueryProcessingPool queryProcessingPool;
  private final ServiceEmitter emitter;
  private final ObjectMapper objectMapper;
  private final SegmentIdWithShardSpec onlySegmentIdWithSharSpec;
  private final Map<DimensionsHolder, DimensionsHolderWithVersion> index;
  private final Set<String> keyColumnsSet;
  private final Map<String, Integer> dimToIndexMap;
  private final AtomicReference<Boolean> shouldTrackSegment;
  private final String baseSnapshotPath;
  private final String snapshotPath;

  public UpdatableStreamAppenderator(
      String id,
      DataSchema schema,
      AppenderatorConfig config,
      StorageConnector storageConnector,
      String dataSource,
      DataSegmentAnnouncer segmentAnnouncer,
      QueryRunnerFactoryConglomerate conglomerate,
      QueryProcessingPool queryProcessingPool,
      ServiceEmitter emitter,
      ObjectMapper objectMapper
  )
  {
    this.id = id;
    this.counter = new AtomicInteger();
    this.schema = schema;
    this.config = config;
    this.storageConnector = storageConnector;
    this.dataSource = dataSource;
    this.updateConfig = (UpdateSpec) config.getAppendableIndexSpec();
    this.segmentAnnouncer = segmentAnnouncer;
    this.conglomerate = conglomerate;
    this.queryProcessingPool = queryProcessingPool;
    this.emitter = emitter;
    this.objectMapper = objectMapper;
    this.onlySegmentIdWithSharSpec = new SegmentIdWithShardSpec(
        dataSource,
        Intervals.ETERNITY,
        UpdateSegmentAllocator.UPDATE_VERSION,
        UpdateSegmentAllocator.SHARD_SPEC
    );
    this.index = new ConcurrentHashMap<>();
    this.keyColumnsSet = Sets.newHashSet(updateConfig.getKeyColumns());
    this.dimToIndexMap = new LinkedHashMap<>();
    this.shouldTrackSegment = new AtomicReference<>(false);
    this.baseSnapshotPath = makeFileSystemPath("dimension-tables", dataSource);
    this.snapshotPath = makeFileSystemPath(
        this.baseSnapshotPath,
        UpdateSegmentAllocator.UPDATE_VERSION,
        "table.snapshot"
    );
    LOGGER.info("Key Columns : [%s], version column : [%s]", updateConfig.getKeyColumns(), updateConfig.getVersionColumn());
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    SegmentDescriptor segmentDescriptor = new SegmentDescriptor(
        onlySegmentIdWithSharSpec.getInterval(),
        onlySegmentIdWithSharSpec.getVersion(),
        onlySegmentIdWithSharSpec.getShardSpec().getPartitionNum()
    );
    return getQueryRunnerForSegments(query, ImmutableList.of(segmentDescriptor));
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    // check that there's only one segment
    List<SegmentDescriptor> segmentDescriptors = Lists.newArrayList(specs.iterator());
    if (segmentDescriptors.size() == 0) {
      return new NoopQueryRunner<>();
    } else if (segmentDescriptors.size() > 1) {
      throw DruidException.defensive("Expected only single segment");
    }

    // We only handle one particular dataSource. Make sure that's what we have, then ignore from here on out.
    final DataSource dataSourceFromQuery = query.getDataSource();
    final DataSourceAnalysis analysis = dataSourceFromQuery.getAnalysis();

    // Sanity check: make sure the query is based on the table we're meant to handle.
    if (!analysis.getBaseTableDataSource().filter(ds -> dataSource.equals(ds.getName())).isPresent()) {
      throw new ISE("Cannot handle datasource: %s", dataSourceFromQuery);
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    // Make sure this query type can handle the subquery, if present.
    if ((dataSourceFromQuery instanceof QueryDataSource) && !toolChest.canPerformSubquery(((QueryDataSource) dataSourceFromQuery).getQuery())) {
      throw new ISE("Cannot handle subquery: %s", dataSourceFromQuery);
    }

    // segmentMapFn maps each base Segment into a joined Segment if necessary.
    final Function<SegmentReference, SegmentReference> segmentMapFn = dataSourceFromQuery
        .createSegmentMapFunction(
            query,
            cpuTimeAccumulator
        );

    // create inline segment
    SegmentDescriptor segmentDescriptor = segmentDescriptors.get(0);
    NonnullPair<List<Object[]>, RowSignature> snapshotValues = getsnapshotValues();
    InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
        snapshotValues.lhs,
        snapshotValues.rhs
    );
    Segment segment = new ArrayListSegment<>(
        SegmentId.of(
            getDataSource(),
            segmentDescriptor.getInterval(),
            segmentDescriptor.getVersion(),
            segmentDescriptor.getPartitionNumber()
        ),
        (ArrayList<Object[]>) inlineDataSource.getRowsAsList(),
        inlineDataSource.rowAdapter(),
        inlineDataSource.getRowSignature()
    );

    SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(segmentDescriptor);
    SegmentId segmentId = segment.getId();
    Interval segmentInterval = segment.getDataInterval();
    String segmentIdString = segmentId.toString();

    MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerInner = new MetricsEmittingQueryRunner<>(
        emitter,
        toolChest,
        new ReferenceCountingSegmentQueryRunner<>(
            factory,
            segmentMapFn.apply(ReferenceCountingSegment.wrapSegment(segment, onlySegmentIdWithSharSpec.getShardSpec())),
            segmentDescriptor
        ),
        QueryMetrics::reportSegmentTime,
        queryMetrics -> queryMetrics.segment(segmentIdString)
    );

    BySegmentQueryRunner<T> bySegmentQueryRunner = new BySegmentQueryRunner<>(
        segmentId,
        segmentInterval.getStart(),
        metricsEmittingQueryRunnerInner
    );

    MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerOuter = new MetricsEmittingQueryRunner<>(
        emitter,
        toolChest,
        bySegmentQueryRunner,
        QueryMetrics::reportSegmentAndCacheTime,
        queryMetrics -> queryMetrics.segment(segmentIdString)
    ).withWaitMeasuredFromNow();

    SpecificSegmentQueryRunner<T> specificSegmentQueryRunner = new SpecificSegmentQueryRunner<>(
        metricsEmittingQueryRunnerOuter,
        segmentSpec
    );

    PerSegmentOptimizingQueryRunner<T> perSegmentOptimizingQueryRunner = new PerSegmentOptimizingQueryRunner<>(
        specificSegmentQueryRunner,
        new PerSegmentQueryOptimizationContext(segmentDescriptor)
    );

    QueryRunner<T> mergedRunner =
        toolChest.mergeResults(
            factory.mergeRunners(
                queryProcessingPool,
                Collections.singletonList(perSegmentOptimizingQueryRunner)
            )
        );

    return CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<>(mergedRunner, toolChest),
        toolChest,
        emitter,
        cpuTimeAccumulator,
        true
    );
  }

  @Override
  public String getId()
  {
    return id;
  }

  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  public Object startJob()
  {
    lockBasePersistDirectory();
    try {
      if (storageConnector.pathExists("myCounter")) {
        try (DataInputStream dataInputStream = new DataInputStream(storageConnector.read("myCounter"))) {
          counter.set(dataInputStream.readInt());
        }
      }
      segmentAnnouncer.announceSegment(
          new DataSegment(
              getDataSource(),
              onlySegmentIdWithSharSpec.getInterval(),
              onlySegmentIdWithSharSpec.getVersion(),
              ImmutableMap.of(),
              Collections.emptyList(),
              Collections.emptyList(),
              onlySegmentIdWithSharSpec.getShardSpec(),
              null,
              0
          )
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    restoreFromSnapshot();
    return null;
  }
  
  private void restoreFromSnapshot()
  {
    String latestVersion = UpdateSnapshotStorageUtils.findLatestSnapshotVersionPath(storageConnector, baseSnapshotPath);
    if (latestVersion == null) {
      LOGGER.info("No snapshot found");
      return;
    }

    String fullRelativePathForSnasphot = makeFileSystemPath(baseSnapshotPath, latestVersion, "table.snapshot");
    NonnullPair<List<Object[]>, RowSignature> snapshotValues =
        UpdateSnapshotStorageUtils.readSnapshotValuesFromStorage(storageConnector, fullRelativePathForSnasphot, objectMapper);
    printSnapshotValues(
        snapshotValues
    );
    LOGGER.info("Restoring snapshot from relative path : [%s]", fullRelativePathForSnasphot);
    LOGGER.info("Row count in snapshot : [%d]", snapshotValues.lhs.size());
    for (Object[] row : snapshotValues.lhs) {
      Map<String, Object> mapRowBuilder = new HashMap<>();
      for (String dim : schema.getDimensionsSpec().getDimensionNames()) {
        if (snapshotValues.rhs.contains(dim)) {
          ColumnType oldColumnType = snapshotValues.rhs.getColumnType(dim).orElse(null);
          boolean columnTypeSame =
              oldColumnType != null && oldColumnType.equals(schema.getDimensionsSpec().getSchema(dim).getColumnType());
          if (columnTypeSame) {
            mapRowBuilder.put(dim, row[snapshotValues.rhs.indexOf(dim)]);
          } else {
            mapRowBuilder.put(dim, null); // TODO : try to coerce to the new type
          }
        } else {
          mapRowBuilder.put(dim, null);
        }
      }
      addToIndex(new MapBasedInputRow(0, schema.getDimensionsSpec().getDimensionNames(), mapRowBuilder), true);
    }
    LOGGER.info("Restored snapshot from relative path : [%s]", fullRelativePathForSnasphot);
    LOGGER.info("Current rows in datasource : [%d]", getTotalRowCount());
  }

  @Override
  public AppenderatorAddResult add(
      SegmentIdWithShardSpec identifier,
      InputRow row,
      @Nullable Supplier<Committer> committerSupplier,
      boolean allowIncrementalPersists
  ) throws IndexSizeExceededException, SegmentNotWritableException
  {
    if (!identifier.equals(onlySegmentIdWithSharSpec)) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build("Found a new segment");
    }
    shouldTrackSegment.compareAndSet(false, true);
    counter.incrementAndGet();
    if (counter.get() % 1000 == 0) {
      LOGGER.info("counter value : " + counter.get() + " identifier : " + identifier);
    }
    addToIndex(row, false);
    return new AppenderatorAddResult(identifier, counter.get(), false);
  }

  private void addToIndex(InputRow row, boolean isSnapshotRow)
  {
    Object[] keyColumns = new Object[updateConfig.getKeyColumns().size()];
    for (int i = 0; i < keyColumns.length; i++) {
      keyColumns[i] = row.getRaw(updateConfig.getKeyColumns().get(i));
    }
    DimensionsHolder keys = new DimensionsHolder(keyColumns);
    DimensionsHolderWithVersion prevValue = index.get(keys);
    long currRowVersion = getVersionValue(row, isSnapshotRow);
    if (prevValue == null || (currRowVersion > prevValue.getVersion())) {
      DimensionsHolderWithVersion values = new DimensionsHolderWithVersion(
          new DimensionsHolder(getDimensionsWithoutKeyColumns(row)),
          currRowVersion
      );
      index.put(keys, values);
    }
  }

  public long getVersionValue(InputRow row, boolean isSnapshotRow)
  {
    if (!isSnapshotRow && updateConfig.getVersionColumn().equals(ColumnHolder.TIME_COLUMN_NAME)) {
      return row.getTimestampFromEpoch();
    }
    try {
      Object rawValue = row.getRaw(updateConfig.getVersionColumn());
      if (rawValue == null) {
        return Long.MIN_VALUE;
      }
      return Long.parseLong(rawValue.toString());
    }
    catch (NumberFormatException e) {
      return Long.MIN_VALUE;
    }
  }

  private Object[] getDimensionsWithoutKeyColumns(InputRow row)
  {
    ObjectArrayList<Object> newDims = new ObjectArrayList<>();
    IntArrayList newDimIndexes = new IntArrayList();
    Object[] dims = new Object[dimToIndexMap.size()];
    for (String dim : row.getDimensions()) {
      if (keyColumnsSet.contains(dim)) {
        continue;
      }
      // add a new dim with last index if the dim is new
      dimToIndexMap.putIfAbsent(dim, dimToIndexMap.size());
      int idxToInsert = dimToIndexMap.get(dim);
      if (idxToInsert >= dims.length) {
        newDims.add(row.getRaw(dim));
        newDimIndexes.add(idxToInsert);
      } else {
        dims[idxToInsert] = row.getRaw(dim);
      }
    }
    if (newDims.size() > 0) {
      Object[] rescaleDims = new Object[dims.length + newDims.size()];
      System.arraycopy(dims, 0, rescaleDims, 0, dims.length);
      for (int i = 0; i < newDims.size(); i++) {
        rescaleDims[newDimIndexes.getInt(i)] = newDims.get(i);
      }
      return rescaleDims;
    }
    return dims;
  }

  @Override
  public List<SegmentIdWithShardSpec> getSegments()
  {
    Boolean returnSegment = shouldTrackSegment.get();
    if (returnSegment != null && returnSegment) {
      // only return the segment once add is called, because it means that the segment is also assigned by the allocator
      return ImmutableList.of(onlySegmentIdWithSharSpec);
    }
    return ImmutableList.of();
  }

  @Override
  public int getRowCount(SegmentIdWithShardSpec identifier)
  {
    return getTotalRowCount();
  }

  @Override
  public int getTotalRowCount()
  {
    return index.size();
  }

  @Override
  public void clear() throws InterruptedException
  {
    index.clear();
  }

  @Override
  public ListenableFuture<?> drop(SegmentIdWithShardSpec identifier)
  {
    // TODO : should we drop the segment here?
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Object> persistAll(@Nullable Committer committer)
  {
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<SegmentsAndCommitMetadata> push(
      Collection<SegmentIdWithShardSpec> identifiers,
      @Nullable Committer committer,
      boolean useUniquePath
  )
  {
    try (DataOutputStream outputStream = new DataOutputStream(storageConnector.write("myCounter"))) {
      outputStream.writeInt(counter.get());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (committer != null) {
      LOGGER.info("committer metadata : " + committer.getMetadata());
    }
    printSnapshotValues(getsnapshotValues());
    // only commit snapshot if you have a segment to push
    if (!identifiers.isEmpty()) {
      UpdateSnapshotStorageUtils.writeSnpashotValuesToStorage(
          getsnapshotValues(),
          storageConnector,
          getSnapshotPath(),
          objectMapper
      );
      UpdateSnapshotStorageUtils.cleanupOldSnapshots(
          storageConnector,
          baseSnapshotPath,
          updateConfig.getSnapshotCountToRetain()
      );
    }

    ImmutableList.Builder<DataSegment> dataSegmentBuilder = ImmutableList.builder();
    for (SegmentIdWithShardSpec spec : identifiers) {
      dataSegmentBuilder.add(
          new DataSegment(
              spec.getDataSource(),
              spec.getInterval(),
              spec.getVersion(),
              ImmutableMap.of(),
              Collections.emptyList(),
              Collections.emptyList(),
              spec.getShardSpec(),
              null,
              0
          )
      );
    }
    SegmentsAndCommitMetadata retVal = new SegmentsAndCommitMetadata(
        dataSegmentBuilder.build(),
        committer == null ? null : committer.getMetadata()
    );
    return Futures.immediateFuture(retVal);
  }

  private void printSnapshotValues(NonnullPair<List<Object[]>, RowSignature> snapshotValues)
  {
    // log map entries
    /*int limit = 0;
    for (Map.Entry<DimensionsHolder, DimensionsHolderWithVersion> entry : index.entrySet()) {
      if (limit > 100) {
        continue;
      }
      limit++;
      LOGGER.info(entry.getKey().toString() + " " + entry.getValue().toString());
    }*/

    // log segment values
    int limit = 0;
    LOGGER.info("Row Signature : " + snapshotValues.rhs);
    for (Object[] row : snapshotValues.lhs) {
      if (limit > 100) {
        continue;
      }
      limit++;
      StringBuilder sb = new StringBuilder();
      for (Object x : row) {
        sb.append(x.getClass()).append(" ");
      }
      LOGGER.info(sb.toString());
      LOGGER.info(Arrays.toString(row));
    }
  }

  @Override
  public void close()
  {
    try {
      segmentAnnouncer.unannounceSegment(
          new DataSegment(
              getDataSource(),
              onlySegmentIdWithSharSpec.getInterval(),
              onlySegmentIdWithSharSpec.getVersion(),
              ImmutableMap.of(),
              Collections.emptyList(),
              Collections.emptyList(),
              onlySegmentIdWithSharSpec.getShardSpec(),
              null,
              0
          )
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    unlockBasePersistDirectory();
  }

  @Override
  public void closeNow()
  {

  }

  private String getSnapshotPath()
  {
    return snapshotPath;
  }

  private void lockBasePersistDirectory()
  {
    if (basePersistDirLock == null) {
      try {
        FileUtils.mkdirp(config.getBasePersistDirectory());

        basePersistDirLockChannel = FileChannel.open(
            computeLockFile().toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
        );

        basePersistDirLock = basePersistDirLockChannel.tryLock();
        if (basePersistDirLock == null) {
          throw new ISE("Cannot acquire lock on basePersistDir: %s", computeLockFile());
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void unlockBasePersistDirectory()
  {
    try {
      if (basePersistDirLock != null) {
        basePersistDirLock.release();
        basePersistDirLockChannel.close();
        basePersistDirLock = null;
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private File computeLockFile()
  {
    return new File(config.getBasePersistDirectory(), ".lock");
  }

  private NonnullPair<List<Object[]>, RowSignature> getsnapshotValues()
  {
    LOGGER.info("dimToIndexMap : ");
    LOGGER.info(dimToIndexMap.toString());
    ArrayList<Map.Entry<String, Integer>> dimList = new ArrayList<>(dimToIndexMap.entrySet());
    ArrayList<Object[]> values = new ArrayList<>();
    for (Map.Entry<DimensionsHolder, DimensionsHolderWithVersion> segmentRow : index.entrySet()) {
      Object[] row = new Object[keyColumnsSet.size() + dimList.size() + 1];
      int columnCounter = 0;
      // add the keys
      System.arraycopy(segmentRow.getKey().getDims(), 0, row, 0, segmentRow.getKey().getDims().length);
      columnCounter += segmentRow.getKey().getDims().length;
      // add __version column
      row[columnCounter++] = segmentRow.getValue().getVersion();
      // add the other dimensions
      int dimCounter = 0;
      for (Map.Entry<String, Integer> dim : dimList) {
        if (dim.getValue() < segmentRow.getValue().getDimensionsHolder().getDims().length) {
          row[columnCounter + dimCounter] = segmentRow.getValue().getDimensionsHolder().getDims()[dim.getValue()];
          dimCounter++;
        }
      }
      values.add(row);
    }

    // currently getting everything from schema
    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    for (String keyColumn : updateConfig.getKeyColumns()) {
      rowSignatureBuilder.add(keyColumn, schema.getDimensionsSpec().getSchema(keyColumn).getColumnType());
    }
    // add __version column
    rowSignatureBuilder.add("__version", ColumnType.LONG);
    for (Map.Entry<String, Integer> dim : dimList) {
      rowSignatureBuilder.add(dim.getKey(), schema.getDimensionsSpec().getSchema(dim.getKey()).getColumnType());
    }
    return new NonnullPair<>(values, rowSignatureBuilder.build());
  }

  static String makeFileSystemPath(String... parts)
  {
    return Joiner.on('/').join(parts);
  }

  private static class DimensionsHolder
  {
    final Object[] dims;

    public DimensionsHolder(Object[] dims)
    {
      this.dims = dims;
    }

    public Object[] getDims()
    {
      return dims;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DimensionsHolder that = (DimensionsHolder) o;
      return Arrays.equals(getDims(), that.getDims());
    }

    @Override
    public int hashCode()
    {
      return Arrays.hashCode(getDims());
    }

    @Override
    public String toString()
    {
      return "DimensionsHolder{" +
             "dims=" + Arrays.toString(dims) +
             '}';
    }
  }

  private static class DimensionsHolderWithVersion
  {
    DimensionsHolder dimensionsHolder;
    long version;

    public DimensionsHolderWithVersion(DimensionsHolder dimensionsHolder, long version)
    {
      this.dimensionsHolder = dimensionsHolder;
      this.version = version;
    }

    public DimensionsHolder getDimensionsHolder()
    {
      return dimensionsHolder;
    }

    public long getVersion()
    {
      return version;
    }

    @Override
    public String toString()
    {
      return "DimensionsHolderWithTime{" +
             "dimensionsHolder=" + dimensionsHolder +
             ", version=" + version +
             '}';
    }
  }
}
