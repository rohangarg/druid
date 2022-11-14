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

package org.apache.druid.frame.processor;

import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ComposingReadableFrameChannel;
import org.apache.druid.frame.channel.ComposingWritableFrameChannel;
import org.apache.druid.frame.channel.PartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class ComposingOutputChannelFactory implements OutputChannelFactory
{
  private final OutputChannelFactory[] channelFactories;
  private final long[] limits;
  private final int frameSize;

  public ComposingOutputChannelFactory(OutputChannelFactory[] channelFactories, long[] limits, int frameSize)
  {
    this.channelFactories = channelFactories;
    this.limits = limits;
    this.frameSize = frameSize;
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    Supplier<WritableFrameChannel>[] writableFrameChannels = new Supplier[limits.length];
    Supplier<ReadableFrameChannel>[] readableFrameChannelSuppliers = new Supplier[limits.length];
    for (int i = 0; i < writableFrameChannels.length; i++) {
      int finalI = i;
      Supplier<OutputChannel> channel =
          Suppliers.memoize(() -> {
            try {
              return channelFactories[finalI].openChannel(partitionNumber);
            }
            catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          })::get;
      writableFrameChannels[i] = () -> channel.get().getWritableChannel();
      readableFrameChannelSuppliers[i] = () -> channel.get().getReadableChannel();
    }
    Map<Integer, HashSet<Integer>> partitionToChannelMap = new HashMap<>();
    ComposingWritableFrameChannel writableFrameChannel = new ComposingWritableFrameChannel(
        writableFrameChannels,
        limits,
        partitionToChannelMap
    );
    Supplier<ReadableFrameChannel> readableFrameChannelSupplier = Suppliers.memoize(
        () -> new ComposingReadableFrameChannel(
            partitionNumber,
            readableFrameChannelSuppliers,
            partitionToChannelMap
        )
    )::get;
    return OutputChannel.pair(
        writableFrameChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        readableFrameChannelSupplier,
        partitionNumber
    );
  }

  @Override
  public PartitionedOutputChannel openChannel(String name, boolean deleteAfterRead, long maxBytes) throws IOException
  {
    Supplier<WritableFrameChannel>[] writableFrameChannels = new Supplier[limits.length];
    Supplier<PartitionedReadableFrameChannel>[] partitionedReaderSuppliers = new Supplier[limits.length];
    for (int i = 0; i < writableFrameChannels.length; i++) {
      int finalI = i;
      Supplier<PartitionedOutputChannel> channel =
          Suppliers.memoize(() -> {
            try {
              return channelFactories[finalI].openChannel(name, deleteAfterRead, Math.min(limits[finalI], maxBytes));
            }
            catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          })::get;
      writableFrameChannels[i] = () -> channel.get().getWritableChannel();
      partitionedReaderSuppliers[i] = () -> channel.get().getReadableChannelSupplier().get();
    }
    Map<Integer, HashSet<Integer>> partitionToChannelMap = new HashMap<>();
    ComposingWritableFrameChannel writableFrameChannel = new ComposingWritableFrameChannel(
        writableFrameChannels,
        limits,
        partitionToChannelMap
    );
    PartitionedReadableFrameChannel partitionedReadableFrameChannel = new PartitionedReadableFrameChannel()
    {
      private final Set<Integer> openedChannels = Sets.newHashSetWithExpectedSize(1);

      @Override
      public ReadableFrameChannel openChannel(int partitionNumber)
      {
        Supplier<ReadableFrameChannel>[] suppliers = new Supplier[partitionedReaderSuppliers.length];
        for (int i = 0; i < partitionedReaderSuppliers.length; i++) {
          int finalI = i;
          suppliers[i] = Suppliers.memoize(
              () -> {
                openedChannels.add(finalI);
                return partitionedReaderSuppliers[finalI].get().openChannel(partitionNumber);
              }
          )::get;
        }

        return new ComposingReadableFrameChannel(partitionNumber, suppliers, partitionToChannelMap);
      }

      @Override
      public void close() throws IOException
      {
        for (Integer channelId : openedChannels) {
          partitionedReaderSuppliers[channelId].get().close();
        }
      }
    };

    return PartitionedOutputChannel.pair(
        writableFrameChannel,
        ArenaMemoryAllocator.createOnHeap(8_000_000),
        () -> partitionedReadableFrameChannel
    );
  }

  @Override
  public OutputChannel openNilChannel(int partitionNumber)
  {
    return OutputChannel.nil(partitionNumber);
  }
}
