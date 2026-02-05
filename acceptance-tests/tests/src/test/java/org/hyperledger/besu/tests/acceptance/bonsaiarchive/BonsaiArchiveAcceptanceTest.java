package org.hyperledger.besu.tests.acceptance.bonsaiarchive;

import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class BonsaiArchiveAcceptanceTest extends AcceptanceTestBase {

    @Test
    public void shouldMineBlocksWithBonsaiArchive() throws Exception {
        final BesuNode validator1 =
                besu.createQbftNode("validator1", false, DataStorageFormat.X_BONSAI_ARCHIVE);
        cluster.start(validator1);
        cluster.verify(blockchain.reachesHeight(validator1, 1));

        // Use genesis account for transfers
        final Account sender = accounts.getPrimaryBenefactor();
        final Account receiver1 = accounts.createAccount("receiver1");
        final Account receiver2 = accounts.createAccount("receiver2");

        // Send transactions across multiple blocks to test flat DB context handling
        validator1.execute(accountTransactions.createTransfer(sender, receiver1, 1));
        cluster.verify(blockchain.reachesHeight(validator1, 2));

        validator1.execute(accountTransactions.createTransfer(sender, receiver2, 2));
        cluster.verify(blockchain.reachesHeight(validator1, 3));

        validator1.execute(accountTransactions.createTransfer(sender, receiver1, 3));
        cluster.verify(blockchain.reachesHeight(validator1, 4));

        validator1.execute(accountTransactions.createTransfer(sender, receiver2, 4));
        cluster.verify(blockchain.reachesHeight(validator1, 5));

        // Verify balances
        cluster.verify(receiver1.balanceEquals(4)); // 1 + 3 ether
        cluster.verify(receiver2.balanceEquals(6)); // 2 + 4 ether

        // Verify archive and non-archive validators see consistent state
        final BigInteger receiver1BalanceArchive1 =
                validator1.execute(ethTransactions.getBalance(receiver1));
        final BigInteger receiver1BalanceArchive2 =
                validator1.execute(ethTransactions.getBalance(receiver1));
        final BigInteger receiver1BalanceBonsai =
                validator1.execute(ethTransactions.getBalance(receiver1));

        assertThat(receiver1BalanceArchive1)
                .isEqualTo(receiver1BalanceArchive2)
                .isEqualTo(receiver1BalanceBonsai);
    }

    @Test
    public void shouldQueryHistoricalStateWithBonsaiArchive() throws Exception {
        // Create mixed network: 2 archive, 2 regular Bonsai
        final BesuNode archiveValidator1 =
                besu.createQbftNode("archiveValidator1", false, DataStorageFormat.X_BONSAI_ARCHIVE);

        cluster.start(archiveValidator1);

        // Wait for blocks
        cluster.verify(blockchain.reachesHeight(archiveValidator1, 1));

        // Use genesis account
        final Account sender = accounts.getPrimaryBenefactor();
        final Account receiver = accounts.createAccount("receiver");

        // Send transactions and track block numbers
        archiveValidator1.execute(accountTransactions.createTransfer(sender, receiver, 1));
        cluster.verify(blockchain.reachesHeight(archiveValidator1, 2));
        final BigInteger block2 = BigInteger.valueOf(2);

        archiveValidator1.execute(accountTransactions.createTransfer(sender, receiver, 2));
        cluster.verify(blockchain.reachesHeight(archiveValidator1, 3));
        final BigInteger block3 = BigInteger.valueOf(3);

        archiveValidator1.execute(accountTransactions.createTransfer(sender, receiver, 3));
        cluster.verify(blockchain.reachesHeight(archiveValidator1, 4));
        final BigInteger block4 = BigInteger.valueOf(4);

        archiveValidator1.execute(accountTransactions.createTransfer(sender, receiver, 4));
        cluster.verify(blockchain.reachesHeight(archiveValidator1, 5));

        // Mine more blocks to create distance
        cluster.verify(blockchain.reachesHeight(archiveValidator1, 15));

        // Verify archive nodes can query historical state at specific blocks
        final BigInteger oneEther = new BigInteger("1000000000000000000");

        final BigInteger balanceAtBlock2 =
                archiveValidator1.execute(ethTransactions.getBalanceAtBlock(receiver, block2));
        assertThat(balanceAtBlock2).isEqualTo(oneEther); // 1 ether

        final BigInteger balanceAtBlock3 =
                archiveValidator1.execute(ethTransactions.getBalanceAtBlock(receiver, block3));
        assertThat(balanceAtBlock3).isEqualTo(oneEther.multiply(BigInteger.valueOf(3))); // 1+2 ether

        final BigInteger balanceAtBlock4 =
                archiveValidator1.execute(ethTransactions.getBalanceAtBlock(receiver, block4));
        assertThat(balanceAtBlock4).isEqualTo(oneEther.multiply(BigInteger.valueOf(6))); // 1+2+3 ether

        // Verify current balance
        final BigInteger currentBalance =
                archiveValidator1.execute(ethTransactions.getBalance(receiver));
        assertThat(currentBalance).isEqualTo(oneEther.multiply(BigInteger.valueOf(10))); // 1+2+3+4 ether

        // Verify validators are in sync
        cluster.verify(bft.validatorsEqual(archiveValidator1, archiveValidator1));
    }

}
