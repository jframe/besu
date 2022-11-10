package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage.Updater;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

public class ChainPruner implements BlockAddedObserver {
  private final BlockchainStorage blockchainStorage;
  private static final long BLOCKS_TO_KEEP = 256;

  public ChainPruner(final BlockchainStorage blockchainStorage) {
    this.blockchainStorage = blockchainStorage;
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    final long blockNumber = event.getBlock().getHeader().getNumber();
    final Set<Hash> forks = blockchainStorage.getForks(blockNumber);
    forks.add(event.getBlock().getHash());

    final Updater updater = blockchainStorage.updater();
    updater.setForks(blockNumber, forks);
    updater.commit();

    if (event.isNewCanonicalHead() && blockNumber > BLOCKS_TO_KEEP) {
      final long pruningMark = blockNumber - BLOCKS_TO_KEEP;
      pruneBlock(pruningMark);
    }
  }

  private void pruneBlock(final long blockNumber) {
    final Collection<Hash> forks = blockchainStorage.getForks(blockNumber);
    final Updater updater = blockchainStorage.updater();
    for (final Hash fork : forks) {
      final Optional<BlockBody> blockBody = blockchainStorage.getBlockBody(fork);
      if (blockBody.isEmpty()) {
        return;
      }

      for (final Transaction tx : blockBody.get().getTransactions()) {
        updater.removeTransactionLocation(tx.getHash());
      }

      updater.removeBlockHeader(fork);
      updater.removeBlockBody(fork);
      updater.removeTransactionReceipts(fork);
    }
    updater.removeBlockHash(blockNumber);
  }
}
