package xademo;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SingleDatabaseExample implements Runnable {

  private final List<Transaction> transactions;
  private final MysqlDataSource dataSource;

  public SingleDatabaseExample(List<Transaction> transactions, MysqlDataSource dataSource) {
    this.transactions = transactions;
    this.dataSource = dataSource;
    dataSource.setDatabaseName("xa_database_0");
  }

    public void run() {
	try { performTransactions(); } catch (Exception e) {
    System.err.println("Single test failure."); }
    }

  public int performTransactions() {
    int successfulTransactions = 0;
    try {
      Connection connection = dataSource.getConnection();
      for (Transaction transaction : transactions) {
        Statement statement = connection.createStatement();
        statement.execute("BEGIN;");
        String foo = transaction.removeMoneyFromSender();
        int rowsUpdated = statement.executeUpdate(transaction.removeMoneyFromSender());
        if (rowsUpdated > 0) {
          statement.executeUpdate(transaction.transactionSql());
          statement.executeUpdate(transaction.depositMoneyInRecipient());
          statement.execute("COMMIT;");
          successfulTransactions++;
        } else {
          statement.execute("ROLLBACK");
        }
        statement.close();
      }
    } catch (SQLException e) {
      System.err.println("SQL Exception in single database; unexpected failure.");
      e.printStackTrace();
    }
    return successfulTransactions;
  }
}
