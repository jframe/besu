/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.tuweni.bytes.Bytes;

public final class RawBlockWithReceiptsIterator implements Iterator<BlockWithReceipts>, Closeable {
  private static final int DEFAULT_INIT_BUFFER_CAPACITY = 1 << 16;

  private final FileChannel fileChannel;
  private ByteBuffer readBuffer;
  private final BlockHeaderFunctions blockHeaderFunctions;

  private BlockWithReceipts next;

  RawBlockWithReceiptsIterator(
      final Path file, final BlockHeaderFunctions blockHeaderFunctions, final int initialCapacity)
      throws IOException {
    this.blockHeaderFunctions = blockHeaderFunctions;
    fileChannel = FileChannel.open(file);
    readBuffer = ByteBuffer.allocate(initialCapacity);
    nextBlockWithReceipts();
  }

  public RawBlockWithReceiptsIterator(
      final Path file, final BlockHeaderFunctions blockHeaderFunctions) throws IOException {
    this(file, blockHeaderFunctions, DEFAULT_INIT_BUFFER_CAPACITY);
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public BlockWithReceipts next() {
    if (next == null) {
      throw new NoSuchElementException("No more blocks in found in the file.");
    }
    final BlockWithReceipts result = next;
    try {
      nextBlockWithReceipts();
    } catch (final IOException ex) {
      throw new IllegalStateException(ex);
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
  }

  private void nextBlockWithReceipts() throws IOException {
    fillReadBuffer();
    int initial = readBuffer.position();
    if (initial > 0) {
      final int length = RLP.calculateSize(Bytes.wrapByteBuffer(readBuffer));
      if (length > readBuffer.capacity()) {
        readBuffer.flip();
        final ByteBuffer newBuffer = ByteBuffer.allocate(2 * length);
        newBuffer.put(readBuffer);
        readBuffer = newBuffer;
        fillReadBuffer();
        initial = readBuffer.position();
      }

      final Bytes rlpBytes = Bytes.wrap(Bytes.wrapByteBuffer(readBuffer, 0, length).toArray());
      final RLPInput rlp = new BytesValueRLPInput(rlpBytes, false);
      rlp.enterList();

      rlp.enterList();
      final BlockHeader header = BlockHeader.readFrom(rlp, blockHeaderFunctions);
      final BlockBody body = BlockBody.readFrom(rlp, blockHeaderFunctions);
      rlp.leaveList();

      final List<TransactionReceipt> transactionReceipts =
          rlp.readList(TransactionReceipt::readFrom);

      final Block block = new Block(header, body);
      next = new BlockWithReceipts(block, transactionReceipts);
      readBuffer.position(length);
      readBuffer.compact();
      readBuffer.position(initial - length);
    } else {
      next = null;
    }
  }

  private void fillReadBuffer() throws IOException {
    fileChannel.read(readBuffer);
  }
}
