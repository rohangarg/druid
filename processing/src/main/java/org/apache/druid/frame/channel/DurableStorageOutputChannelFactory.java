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

package org.apache.druid.frame.channel;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.io.CountingOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.file.FrameFileFooter;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MappedByteBufferHandler;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class DurableStorageOutputChannelFactory implements OutputChannelFactory
{
  private final String controllerTaskId;
  private final String workerTaskId;
  private final int stageNumber;
  private final int frameSize;
  private final StorageConnector storageConnector;
  private final File tmpDir;
  private final ExecutorService remoteInputStreamPool;

  public DurableStorageOutputChannelFactory(
      final String controllerTaskId,
      final String workerTaskId,
      final int stageNumber,
      final int frameSize,
      final StorageConnector storageConnector,
      final File tmpDir
  )
  {
    this.controllerTaskId = Preconditions.checkNotNull(controllerTaskId, "controllerTaskId");
    this.workerTaskId = Preconditions.checkNotNull(workerTaskId, "workerTaskId");
    this.stageNumber = stageNumber;
    this.frameSize = frameSize;
    this.storageConnector = Preconditions.checkNotNull(storageConnector, "storageConnector");
    this.tmpDir = Preconditions.checkNotNull(tmpDir, "tmpDir is null");
    this.remoteInputStreamPool =
        Executors.newCachedThreadPool(Execs.makeThreadFactory("-remote-fetcher-%d"));
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
      final StorageConnector storageConnector,
      final File tmpDir
  )
  {
    return new DurableStorageOutputChannelFactory(
        controllerTaskId,
        workerTaskId,
        stageNumber,
        frameSize,
        storageConnector,
        tmpDir
    );
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    final String fullFileName = getFilePath(controllerTaskId, workerTaskId, stageNumber, getPartitionName(partitionNumber));
    final WritableFrameFileChannel writableChannel =
        new WritableFrameFileChannel(
            FrameFileWriter.open(
                Channels.newChannel(storageConnector.write(fullFileName)),
                null,
                Long.MAX_VALUE
            )
        );

    return OutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        () -> {
          try {
            RetryUtils.retry(() -> {
              if (!storageConnector.pathExists(fullFileName)) {
                throw new ISE("File does not exist : %s", fullFileName);
              }
              return Boolean.TRUE;
            }, (throwable) -> true, 10);
          }
          catch (Exception exception) {
            throw new RuntimeException(exception);
          }
          try {
            return ReadableInputStreamFrameChannel.open(
                storageConnector.read(fullFileName),
                fullFileName,
                remoteInputStreamPool,
                false
            );
          }
          catch (IOException e) {
            throw new UncheckedIOException(StringUtils.format("Unable to read file : %s", fullFileName), e);
          }
        },
        partitionNumber
    );
  }

  @Override
  public PartitionedOutputChannel openChannel(String name, boolean deleteAfterRead, long maxBytes) throws IOException
  {
    final String fullFileName = getFilePath(controllerTaskId, workerTaskId, stageNumber, name);
    final CountingOutputStream countingOutputStream = new CountingOutputStream(storageConnector.write(fullFileName));
    final WritableFrameFileChannel writableChannel =
        new WritableFrameFileChannel(
            FrameFileWriter.open(
                Channels.newChannel(countingOutputStream),
                ByteBuffer.allocate(Frame.compressionBufferSize(frameSize)),
                maxBytes
            )
        );

    final Supplier<Long> channelSizeSupplier = countingOutputStream::getCount;

    // build supplier for reader the footer of the underlying frame file
    final Supplier<FrameFileFooter> frameFileFooterSupplier = Suppliers.memoize(() -> {
      try {
        // read trailer and find the footer size
        byte[] trailerBytes = new byte[FrameFileWriter.TRAILER_LENGTH];
        long channelSize = channelSizeSupplier.get();
        try (InputStream reader = storageConnector.rangeRead(
            fullFileName,
            channelSize - FrameFileWriter.TRAILER_LENGTH,
            FrameFileWriter.TRAILER_LENGTH
        )) {
          int bytesRead = reader.read(trailerBytes, 0, trailerBytes.length);
          if (bytesRead != FrameFileWriter.TRAILER_LENGTH) {
            throw new RuntimeException("Invalid frame file trailer for object : " + fullFileName);
          }
        }

        Memory trailer = Memory.wrap(trailerBytes);
        int footerLength = trailer.getInt(Integer.BYTES * 2L);

        // read the footer into a file and map it to memory
        File footerFile = new File(tmpDir, fullFileName + "_footer");
        try(FileOutputStream footerFileStream = new FileOutputStream(footerFile);
            InputStream footerInputStream =
                storageConnector.rangeRead(fullFileName, channelSize - footerLength, footerLength)) {
          IOUtils.copy(footerInputStream, footerFileStream);
        }
        MappedByteBufferHandler mapHandle = FileUtils.map(footerFile);
        Memory footerMemory = Memory.wrap(mapHandle.get(), ByteOrder.LITTLE_ENDIAN);

        // create a frame file footer from the mapper memory
        return new FrameFileFooter(footerMemory, channelSize);
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    })::get;

    return PartitionedOutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        () -> new DurableStoragePartitionedReadableFrameChannel(
            storageConnector,
            frameFileFooterSupplier,
            fullFileName,
            remoteInputStreamPool
        )
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
