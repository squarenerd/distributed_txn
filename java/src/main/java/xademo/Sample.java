package xademo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Sample {
  public static void main(String[] args) throws Exception {
    try {
      if (args.length != 4) {
        System.err
            .println(
                "Missing args (noOfDatabases noOfUsers noOfTransactions concurrency)");
        System.exit(1);
      }
      int numberOfDatabases = Integer.parseInt(args[0]);
      int numberOfUsers = Integer.parseInt(args[1]);
      int numberOfTransactions = Integer.parseInt(args[2]);
      int concurrency = Integer.parseInt(args[3]);

      Setup setup = new Setup(numberOfDatabases, numberOfUsers);
      setup.createDatabases();
      setup.seedDatabases();

      // Generate a sample set of transactions.
      ArrayList<Transaction> sampleTransactions = new ArrayList<Transaction>();
      for (int i = 0; i < numberOfTransactions; i++) {
        sampleTransactions.add(setup.randomTransaction());
      }
      // time how long it takes to process those in the single DB using transactions.
      int transactionsPerThread = numberOfTransactions/concurrency;
      // Break the transactions into #concurrency groups
      long singleStartTime = System.currentTimeMillis();
      ExecutorService executor = Executors.newFixedThreadPool(concurrency);
      for (int i = 0; i < concurrency; i++) {
        int startingIndex = i*transactionsPerThread;
        int endingIndex = i==concurrency-1 ? sampleTransactions.size() : (i+1)*transactionsPerThread;
        List<Transaction>subTransactions = sampleTransactions.subList(startingIndex, endingIndex);
        Runnable worker = new SingleDatabaseExample(subTransactions, setup.getDataSource());
        executor.execute(worker);
      }
      executor.shutdown();
      executor.awaitTermination(600, TimeUnit.SECONDS);
      long singleEndTime = System.currentTimeMillis();
      final long singleTestDuration = singleEndTime - singleStartTime;
      System.out.println("The single DB case took " + singleTestDuration + " milliseconds");

      System.out.println("Now performing multiple DB case");
      // time how long it takes to process those in the multi DB using transactions.
      long multiStartTime = System.currentTimeMillis();

      ExecutorService executor2 = Executors.newFixedThreadPool(concurrency);
      for (int i = 0; i < concurrency; i++) {
        int startingIndex = i*transactionsPerThread;
        int endingIndex = i==concurrency-1 ? sampleTransactions.size() : (i+1)*transactionsPerThread;
        List<Transaction>subTransactions = sampleTransactions.subList(startingIndex, endingIndex);
        Runnable worker = new MultiDatabaseExample(subTransactions, setup);
        executor2.execute(worker);
      }
      executor2.shutdown();
      executor2.awaitTermination(600, TimeUnit.SECONDS);
      long multiFinishTime = System.currentTimeMillis();

      final long multiTestDuration = multiFinishTime - multiStartTime;
      System.out.println("Elapsed time: " + multiTestDuration + " milliseconds");

      String nicelyFormattedOutput =
          String.format("%d%c%d%c%d%c%d%c%d%c%d", numberOfDatabases, '\t', numberOfUsers, '\t',
              numberOfTransactions, '\t', concurrency, '\t', singleTestDuration, '\t',
              multiTestDuration);
      System.out.println("#databases, #users,#txns, concurrency, singleduration, multiduration");
      System.out.println(nicelyFormattedOutput);
      setup.cleanDatabases();
    } catch (Exception e) {
      System.err.println("Exception: " + e);
      e.printStackTrace();
    }
  }
}
