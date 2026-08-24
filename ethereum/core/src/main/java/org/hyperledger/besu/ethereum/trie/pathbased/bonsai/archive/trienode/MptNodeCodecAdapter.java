/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;
import org.hyperledger.besu.ethereum.trie.patricia.TrieNodeDecoder;

import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;

/**
 * {@link NodeCodecAdapter} for Merkle Patricia Trie (MPT) nodes. Parses via {@link TrieNodeDecoder}
 * and re-encodes via {@link BytesValueRLPOutput} and {@link CompactEncoding}, mirroring the exact
 * byte layout of the original node.
 *
 * <p>Byte-exactness guarantee: {@code encode(parse(x)).equals(x)} for every canonical MPT node.
 */
public final class MptNodeCodecAdapter implements NodeCodecAdapter {

  /** Singleton instance. */
  public static final MptNodeCodecAdapter INSTANCE = new MptNodeCodecAdapter();

  /** RLP encoding of an empty/null slot — the output of {@code writeNull()}. */
  private static final Bytes RLP_EMPTY = Bytes.of((byte) 0x80);

  private MptNodeCodecAdapter() {}

  @Override
  public int arity() {
    return 16;
  }

  @Override
  public int formatTag() {
    return 0;
  }

  @Override
  public NodeLog parse(final Bytes nodeBytes) {
    final Node<Bytes> node = TrieNodeDecoder.decode(null, nodeBytes);
    if (node instanceof BranchNode<Bytes> branch) {
      final TreeMap<Integer, Bytes> children = new TreeMap<>();
      final List<Node<Bytes>> slots = branch.getChildren();
      for (int i = 0; i < 16; i++) {
        final Bytes ref = slots.get(i).getEncodedBytesRef();
        if (!ref.equals(RLP_EMPTY)) {
          children.put(i, ref);
        }
      }
      return new NodeLog(NodeLog.NodeType.BRANCH, Bytes.EMPTY, children, branch.getValue());
    }
    if (node instanceof ExtensionNode<Bytes> ext) {
      final TreeMap<Integer, Bytes> children = new TreeMap<>();
      children.put(0, ext.getChildren().get(0).getEncodedBytesRef());
      return new NodeLog(NodeLog.NodeType.EXTENSION, ext.getPath(), children, Optional.empty());
    }
    if (node instanceof LeafNode<Bytes> leaf) {
      return new NodeLog(NodeLog.NodeType.LEAF, leaf.getPath(), new TreeMap<>(), leaf.getValue());
    }
    throw new IllegalArgumentException("unsupported MPT node type: " + node.getClass());
  }

  @Override
  public Bytes encode(final NodeLog model) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    switch (model.type()) {
      case BRANCH -> {
        out.startList();
        for (int i = 0; i < 16; i++) {
          final Bytes ref = model.children().get(i);
          if (ref != null) {
            out.writeRaw(ref);
          } else {
            out.writeNull();
          }
        }
        if (model.value().isPresent()) {
          out.writeBytes(model.value().get());
        } else {
          out.writeNull();
        }
        out.endList();
      }
      case EXTENSION -> {
        out.startList();
        out.writeBytes(CompactEncoding.encode(model.path()));
        out.writeRaw(model.children().get(0));
        out.endList();
      }
      case LEAF -> {
        out.startList();
        out.writeBytes(CompactEncoding.encode(model.path()));
        out.writeBytes(model.value().orElse(Bytes.EMPTY));
        out.endList();
      }
    }
    return out.encoded();
  }
}
