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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.storage.StorageConnector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class UpdateSnapshotStorageUtils
{
  private static final Logger LOGGER = new Logger(UpdateSnapshotStorageUtils.class);

  public static boolean writeSnpashotValuesToStorage(
      NonnullPair<List<Object[]>, RowSignature> snapshotValues,
      StorageConnector storageConnector,
      String snapshotPath,
      ObjectMapper objectMapper
  )
  {
    try (BufferedOutputStream outputStream = new BufferedOutputStream(storageConnector.write(snapshotPath))) {
      outputStream.write(objectMapper.writeValueAsBytes(snapshotValues.rhs));
      outputStream.write("\n".getBytes(StandardCharsets.UTF_8));

      for (Object[] row : snapshotValues.lhs) {
        outputStream.write(objectMapper.writeValueAsBytes(row));
        outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
      }
      return true;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static NonnullPair<List<Object[]>, RowSignature> readSnapshotValuesFromStorage(
      StorageConnector storageConnector,
      String snapshotPath,
      ObjectMapper objectMapper
  )
  {
    TypeReference<RowSignature> rowSignatureTypeReference = new TypeReference<RowSignature>() {};
    TypeReference<Object[]> objectArrayTypeReference = new TypeReference<Object[]>() {};
    try {
      if (storageConnector.pathExists(snapshotPath)) {
        try (BufferedReader inputReader = new BufferedReader(
            new InputStreamReader(
                new BufferedInputStream(storageConnector.read(snapshotPath)),
                StandardCharsets.UTF_8
            )
        )) {
          RowSignature rowSignature = objectMapper.readValue(inputReader.readLine(), rowSignatureTypeReference);
          Iterator<String> rows = inputReader.lines().iterator();
          ImmutableList.Builder<Object[]> objectRowsBuilder = ImmutableList.builder();
          while (rows.hasNext()) {
            Object[] row = objectMapper.readValue(rows.next(), objectArrayTypeReference);
            objectRowsBuilder.add(row);
          }
          return new NonnullPair<>(objectRowsBuilder.build(), rowSignature);
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new NonnullPair<>(ImmutableList.of(), RowSignature.empty());
  }

  @Nullable
  public static String findLatestSnapshotVersionPath(StorageConnector storageConnector, String baseSnapshotPath)
  {
    try {
      DateTime latestVersion = null;
      if (!storageConnector.pathExists(baseSnapshotPath)) {
        return null;
      }
      Iterator<String> versions = storageConnector.listDir(baseSnapshotPath);
      while (versions.hasNext()) {
        DateTime currVersion = DateTimes.ISO_DATE_TIME.parse(versions.next().split("/")[0]);
        if (latestVersion == null || (currVersion != null && currVersion.isAfter(latestVersion))) {
          latestVersion = currVersion;
        }
      }
      return latestVersion == null ? null : latestVersion.toString();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void cleanupOldSnapshots(
      StorageConnector storageConnector,
      String baseSnapshotPath,
      int snapshotsToRetain
  )
  {
    try {
      if (!storageConnector.pathExists(baseSnapshotPath)) {
        return;
      }
      Iterator<String> versions = storageConnector.listDir(baseSnapshotPath);
      Set<DateTime> uniqueVersions = new HashSet<>();
      while (versions.hasNext()) {
        DateTime currVersion = DateTimes.ISO_DATE_TIME.parse(versions.next().split("/")[0]);
        if (currVersion != null) {
          uniqueVersions.add(currVersion);
        }
      }
      ArrayList<DateTime> sortedVersions = new ArrayList<>(uniqueVersions);
      sortedVersions.sort(DateTimeComparator.getInstance());
      Collections.reverse(sortedVersions);
      for (int i = snapshotsToRetain; i < sortedVersions.size(); i++) {
        String snapshotPath =
            UpdatableStreamAppenderator.makeFileSystemPath(baseSnapshotPath, sortedVersions.get(i).toString());
        storageConnector.deleteRecursively(snapshotPath);
        LOGGER.info("Deleted snapshot at : [%s]", snapshotPath);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
