package xademo;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;

/**
 * Create a number of identical schemas, only differentiated by database number
 * All names will be of the form xa_database_# going from 1 up to n inclusive
 * There will also be xa_schema_0 that represents a non-sharded example.
 *
 * Finally, we also create a user_service database representing a highly available user service.
 */
public class Setup {
  private final int numberOfDatabases;
  private final int numberOfUsers;
  private MysqlDataSource dataSource;
  private HashMap<Integer, Integer> userShards;
  private final static String HOST = "localhost";
  private final static String USER = "root";
  private final static String PASSWORD = "";

  public Setup(int numberOfDatabases, int numberOfUsers) {
    this.numberOfDatabases = numberOfDatabases;
    this.numberOfUsers = numberOfUsers;
    dataSource = new MysqlDataSource();
    dataSource.setServerName(HOST);
    dataSource.setUser(USER);
    dataSource.setPassword(PASSWORD);
    userShards = new HashMap<Integer, Integer>();
  }

  public MysqlDataSource getDataSource() {
    MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setServerName(HOST);
    dataSource.setUser(USER);
    dataSource.setPassword(PASSWORD);
    return dataSource;
  }

  public MysqlXADataSource getXADataSource() {
    MysqlXADataSource xaDataSource = new MysqlXADataSource();
    xaDataSource.setServerName(HOST);
    xaDataSource.setUser(USER);
    xaDataSource.setPassword(PASSWORD);
    return xaDataSource;
  }

  /**
   * Create all of the databases with the correct schemas.
   */
  public void createDatabases() throws Exception {
    Connection conn = dataSource.getConnection();
    Statement statement = conn.createStatement();
    statement.executeUpdate("DROP DATABASE IF EXISTS user_service");
    statement.executeUpdate("CREATE DATABASE user_service");
    statement.executeUpdate("CREATE TABLE user_service.all_users ("
        + "  id INT(11) NOT NULL AUTO_INCREMENT,"
        + "  email_address VARCHAR(255) NOT NULL DEFAULT '',"
        + "  user_shard INT(11) NOT NULL,"
        + "  PRIMARY KEY (id)"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8");
    // Now generate databases 0 through numberOfDatabases
    for (int i=0; i<=numberOfDatabases; i++) {
      final String dbName = "xa_database_"+i;
      statement.executeUpdate("DROP DATABASE IF EXISTS "+dbName);

      statement.executeUpdate("CREATE DATABASE "+dbName);

      final String createUserBalances = "CREATE TABLE "
          + dbName
          + ".user_balances ("
          + "  id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,"
          + "  current_balance BIGINT(20) NOT NULL,"
          + "  user_email VARCHAR(255) NOT NULL DEFAULT '',"
          + "  PRIMARY KEY (id)"
          + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
      statement.executeUpdate(createUserBalances);
      final String createTransactions = "CREATE TABLE "
          + dbName
          + ".transactions ("
          + "  id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,"
          + "  transaction_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,"
          + "  source_user_id INT(11) NOT NULL,"
          + "  destination_user_id INT(11) NOT NULL,"
          + "  amount_transferred BIGINT(20) NOT NULL,"
          + "  PRIMARY KEY (id)"
          + ") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8";
      statement.executeUpdate(createTransactions);
    }
  }

  /**
   * Seed the data correctly, noting that users are sharded, but xa_schema_0 also has all users
   * to represent a single database version.
   */
  public void seedDatabases() throws Exception {
    // Generate all users and initial balances, also inserting into correct shards
    Connection connection = dataSource.getConnection();
    Statement statement = connection.createStatement();
    for (int i=0; i<numberOfUsers; i++) {
      int userId = i+1;
      String userEmail = "user_"+i+"@example.com";
      long userBalance = 4000L;
      int userShard = (int)(Math.random()*numberOfDatabases+1);
      userShards.put(userId, userShard);
      statement.executeUpdate("INSERT INTO user_service.all_users(id,email_address,user_shard) "
          + "VALUES ("+userId+",'"+userEmail+"',"+userShard+")");
      final String dbName = "xa_database_"+userShard;
      statement.executeUpdate("INSERT INTO "+dbName+".user_balances(id,current_balance,user_email) "+
          "VALUES ("+userId+","+userBalance+",'"+userEmail+"')");
      statement.executeUpdate(
          "INSERT INTO xa_database_0.user_balances(id,current_balance,user_email) " +
              "VALUES (" + userId + "," + userBalance + ",'" + userEmail + "')");
    }
  }

  public void cleanDatabases() throws Exception {
    Connection conn = dataSource.getConnection();
    Statement statement = conn.createStatement();
    statement.executeUpdate("DROP DATABASE user_service");
    // Drop databases 0 through numberOfDatabases
    for (int i=0; i<=numberOfDatabases; i++) {
      final String dbName = "xa_database_"+i;
      statement.executeUpdate("DROP DATABASE "+dbName);
    }
  }

  /**
   * Generate random transactions.  Be certain that the source and destination shards are different;
   * The multi-database code will fail trying to get two locks on the same DB.
   * @return A transaction valued between 500 and 5500 between users on different shards.
   */
  public Transaction randomTransaction() {
    int sourceUserId, destinationUserId;
    do {
      sourceUserId = (int)(Math.random()*numberOfUsers+1);
      destinationUserId = (int)(Math.random()*numberOfUsers+1);
    } while (userShards.get(sourceUserId) == userShards.get(destinationUserId));
    long xferAmount = (long)(Math.random()*5000+500);
    return new Transaction(sourceUserId, destinationUserId, xferAmount);
  }

  public HashMap<Integer, Integer> getUserShards() {
    return userShards;
  }
  public int getNumberOfDatabases() {
    return numberOfDatabases;
  }
}
