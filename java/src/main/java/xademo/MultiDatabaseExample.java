package xademo;

import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXid;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class MultiDatabaseExample implements Runnable {
  private final List<Transaction> transactions;
  private final HashMap<Integer, Integer> userShards;
  private final Setup setup;

  public MultiDatabaseExample(List<Transaction> transactions, Setup setup) {
    this.transactions = transactions;
    this.userShards = setup.getUserShards();
    this.setup = setup;
  }

  public void run() {
    try {
      performTransactions();
    } catch (Exception e) {
      System.err.println("Multidatabase failure " + e);
    }
  }

  static int xid = 0;

  static synchronized int getXid() {
    xid++;
    return xid;
  }

  public int performTransactions() throws Exception {
    int successfulTransactions = 0;

    System.err.println("About to perform Multi DB transactions");
    for (Transaction transaction : transactions) {
      try {
        String sourceDBName = "xa_database_" + userShards.get(transaction.sourceUserId);
        String destinationDBName = "xa_database_" + userShards.get(transaction.destinationUserId);
        MysqlXADataSource mysqlSourceDB = setup.getXADataSource();
        mysqlSourceDB.setDatabaseName(sourceDBName);
        MysqlXADataSource mysqlDestinationDB = setup.getXADataSource();
        mysqlDestinationDB.setDatabaseName(destinationDBName);
        XAConnection sourceXAConnection = mysqlSourceDB.getXAConnection();
        XAConnection destinationXAConnection = mysqlDestinationDB.getXAConnection();
        Connection conn1 = sourceXAConnection.getConnection();
        Connection conn2 = destinationXAConnection.getConnection();
        XAResource xar1 = sourceXAConnection.getXAResource();
        XAResource xar2 = destinationXAConnection.getXAResource();
        Xid xid1 = createXid(getXid());
        Xid xid2 = createXid(getXid());
        xar1.start(xid1, XAResource.TMNOFLAGS);
        xar2.start(xid2, XAResource.TMNOFLAGS);
        Statement sourceStatement = conn1.createStatement();
        Statement destinationStatement = conn2.createStatement();
        boolean do_commit = true;
        int rowsUpdated = sourceStatement.executeUpdate(transaction.removeMoneyFromSender());
        if (rowsUpdated > 0) {
          sourceStatement.executeUpdate(transaction.transactionSql());
          destinationStatement.executeUpdate(transaction.transactionSql());
          destinationStatement.executeUpdate(transaction.depositMoneyInRecipient());
        } else {
          do_commit = false; // Overdrawn account.
        }

        // END both the branches -- THIS IS MUST
        xar1.end(xid1, do_commit ? XAResource.TMSUCCESS : XAResource.TMFAIL);
        xar2.end(xid2, do_commit ? XAResource.TMSUCCESS : XAResource.TMFAIL);

        // Prepare the RMs
        int prp1 = do_commit ? xar1.prepare(xid1) : 0;
        int prp2 = do_commit ? xar2.prepare(xid2) : 0;

        if (prp1 != XAResource.XA_OK || prp2 != XAResource.XA_OK) {
          do_commit = false;
        }

        if (do_commit) {
          xar1.commit(xid1, false);
          xar2.commit(xid2, false);
          successfulTransactions++;
        } else {
          xar1.rollback(xid1);
          xar2.rollback(xid2);
        }
        // Close connections
        conn1.close();
        conn2.close();

        sourceXAConnection.close();
        destinationXAConnection.close();
      } catch (SQLException e) {
        System.err.println("SQL Exception for multi database; unexpected failure.");
        e.printStackTrace();
        return -1;
      }
    }
    return successfulTransactions;
  }

  Xid createXid(int bids) throws XAException {
    byte[] gid = new byte[1];
    gid[0] = (byte) 9;
    byte[] bid = new byte[1];
    bid[0] = (byte) bids;
    byte[] gtrid = new byte[64];
    byte[] bqual = new byte[64];
    System.arraycopy(gid, 0, gtrid, 0, 1);
    System.arraycopy(bid, 0, bqual, 0, 1);
    Xid xid = new MysqlXid(gtrid, bqual, 0x1234);
    return xid;
  }
}
