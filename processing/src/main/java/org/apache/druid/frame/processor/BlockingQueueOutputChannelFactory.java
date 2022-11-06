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

import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.java.util.common.IAE;

import java.io.IOException;

/**
 * An {@link OutputChannelFactory} that generates {@link BlockingQueueFrameChannel}.
 */
public class BlockingQueueOutputChannelFactory implements OutputChannelFactory
{
  private final int frameSize;

  public BlockingQueueOutputChannelFactory(final int frameSize)
  {
    this.frameSize = frameSize;
  }

  @Override
  public OutputChannel openChannel(final int partitionNumber)
  {
    final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
    return OutputChannel.pair(
        channel.writable(),
        ArenaMemoryAllocator.createOnHeap(frameSize),
        channel::readable,
        partitionNumber
    );
  }

  @Override
  public PartitionedOutputChannel openChannel(String name, boolean deleteAfterRead, long maxBytes) throws IOException
  {
    final int maxFrameCount = (int) Math.ceil((double) maxBytes / frameSize);
    final BlockingQueueFrameChannel channel = new BlockingQueueFrameChannel(maxFrameCount);
    return PartitionedOutputChannel.pair(
        channel.writable(),
        ArenaMemoryAllocator.createOnHeap(frameSize),
        () -> (partition) -> {
          if (partition != FrameWithPartition.NO_PARTITION) {
            throw new IAE("Invalid partition");
          }
          return channel.readable();
        }
    );
  }

  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    return OutputChannel.nil(partitionNumber);
  }
}
