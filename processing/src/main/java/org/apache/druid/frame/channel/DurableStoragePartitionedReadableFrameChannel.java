package org.apache.druid.frame.channel;

import org.apache.druid.frame.file.FrameFileFooter;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.storage.StorageConnector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public class DurableStoragePartitionedReadableFrameChannel implements PartitionedReadableFrameChannel
{
  private final StorageConnector storageConnector;
  private final Supplier<FrameFileFooter> frameFileFooterSupplier;
  private final String frameFileFullPath;
  private final ExecutorService remoteInputStreamPool;

  public DurableStoragePartitionedReadableFrameChannel(
      StorageConnector storageConnector,
      Supplier<FrameFileFooter> frameFileFooterSupplier,
      String frameFileFullPath,
      ExecutorService remoteInputStreamPool
  )
  {
    this.storageConnector = storageConnector;
    this.frameFileFooterSupplier = frameFileFooterSupplier;
    this.frameFileFullPath = frameFileFullPath;
    this.remoteInputStreamPool = remoteInputStreamPool;
  }

  @Override
  public ReadableFrameChannel openChannel(int partitionNumber)
  {
    FrameFileFooter frameFileFooter = frameFileFooterSupplier.get();
    // find the range to read for partition
    int startFrame = frameFileFooter.getPartitionStartFrame(partitionNumber);
    int endFrame = frameFileFooter.getPartitionStartFrame(partitionNumber + 1);
    long startByte = startFrame == 0 ? FrameFileWriter.MAGIC.length : frameFileFooter.getFrameEndPosition(startFrame - 1);
    long endByte = frameFileFooter.getFrameEndPosition(endFrame - 1);

    try {
      return ReadableInputStreamFrameChannel.open(
          storageConnector.rangeRead(frameFileFullPath, startByte, endByte - startByte),
          frameFileFullPath,
          remoteInputStreamPool,
          true
      );
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() throws IOException
  {
    try {
      storageConnector.deleteFile(frameFileFullPath);
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
