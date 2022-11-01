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

package org.apache.druid.msq.shuffle;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableInputStreamFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.StorageConnector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;

public class DurableStorageOutputChannelFactory implements OutputChannelFactory
{
  private final String controllerTaskId;
  private final String workerTaskId;
  private final int stageNumber;
  private final int frameSize;
  private final StorageConnector storageConnector;

  public DurableStorageOutputChannelFactory(
      final String controllerTaskId,
      final String workerTaskId,
      final int stageNumber,
      final int frameSize,
      final StorageConnector storageConnector
  )
  {
    this.controllerTaskId = Preconditions.checkNotNull(controllerTaskId, "controllerTaskId");
    this.workerTaskId = Preconditions.checkNotNull(workerTaskId, "workerTaskId");
    this.stageNumber = stageNumber;
    this.frameSize = frameSize;
    this.storageConnector = Preconditions.checkNotNull(storageConnector, "storageConnector");
  }

  /**
   * Creates an instance that is the standard production implementation. Closeable items are registered with
   * the provided Closer.
   */
  public static DurableStorageOutputChannelFactory createStandardImplementation(
      final String controllerTaskId,
      final String workerTaskId,
      final int stageNumber,
      final int frameSize,
      final StorageConnector storageConnector
  )
  {
    return new DurableStorageOutputChannelFactory(
        controllerTaskId,
        workerTaskId,
        stageNumber,
        frameSize,
        storageConnector
    );
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    return buildChannel(getPartitionName(partitionNumber), false, partitionNumber, Long.MAX_VALUE);
  }

  @Override
  public OutputChannel openChannel(String name, boolean deleteAfterRead, long maxBytes) throws IOException
  {
    return buildChannel(name, deleteAfterRead, FrameWithPartition.NO_PARTITION, maxBytes);
  }

  private OutputChannel buildChannel(
      String fileName,
      boolean deleteAfterRead,
      int partitionNumber,
      long maxBytes
  ) throws IOException
  {
    final String fullFileName = getFilePath(controllerTaskId, workerTaskId, stageNumber, fileName);
    final WritableFrameFileChannel writableChannel =
        new WritableFrameFileChannel(
            FrameFileWriter.open(
                Channels.newChannel(storageConnector.write(fullFileName)),
                null,
                maxBytes
            )
        );

    return OutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        () -> {
          try {
            ReadableFrameChannel delegate = ReadableInputStreamFrameChannel.open(
                storageConnector.read(fullFileName),
                fullFileName,
                null
            );
            // created to add deletion of channel data upon close of readable channel
            return new ReadableFrameChannel()
            {
              @Override
              public boolean isFinished()
              {
                return delegate.isFinished();
              }

              @Override
              public boolean canRead()
              {
                return delegate.canRead();
              }

              @Override
              public Frame read()
              {
                return delegate.read();
              }

              @Override
              public ListenableFuture<?> readabilityFuture()
              {
                return delegate.readabilityFuture();
              }

              @Override
              public void close()
              {
                if (deleteAfterRead) {
                  try {
                    storageConnector.deleteFile(fullFileName);
                  }
                  catch (IOException e) {
                    delegate.close();
                    throw new UncheckedIOException(e);
                  }
                }
                delegate.close();
              }
            };
          }
          catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        },
        partitionNumber
    );
  }

  @Override
  public OutputChannel openNilChannel(int partitionNumber)
  {
    final String fileName = getFilePath(controllerTaskId, workerTaskId, stageNumber, getPartitionName(partitionNumber));
    // As tasks dependent on output of this partition will forever block if no file is present in RemoteStorage. Hence, writing a dummy frame.
    try {

      FrameFileWriter.open(Channels.newChannel(storageConnector.write(fileName)), null).close();
      return OutputChannel.nil(partitionNumber);
    }
    catch (IOException e) {
      throw new ISE(
          e,
          "Unable to create empty remote output of workerTask[%s] stage[%d] partition[%d]",
          workerTaskId,
          stageNumber,
          partitionNumber
      );
    }
  }

  public static String getControllerDirectory(final String controllerTaskId)
  {
    return StringUtils.format("controller_%s", IdUtils.validateId("controller task ID", controllerTaskId));
  }

  public static String getFilePath(
      final String controllerTaskId,
      final String workerTaskId,
      final int stageNumber,
      final String fileName
  )
  {
    return StringUtils.format(
        "%s/worker_%s/stage_%d/%s",
        getControllerDirectory(controllerTaskId),
        IdUtils.validateId("worker task ID", workerTaskId),
        stageNumber,
        fileName
    );
  }

  public static String getPartitionName(final int partitionNumber)
  {
    return String.format("part_%d", partitionNumber);
  }
}
