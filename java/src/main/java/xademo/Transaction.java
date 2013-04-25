package xademo;

public class Transaction {
  public int sourceUserId;
  public int destinationUserId;
  public long amount;

  public Transaction(int sourceUserId, int destinationUserId, long amount) {
    this.sourceUserId = sourceUserId;
    this.destinationUserId = destinationUserId;
    this.amount = amount;
  }

  public String transactionSql() {
    return "insert into transactions(transaction_date, source_user_id, destination_user_id, amount_transferred) "
        + "values (now(), "+sourceUserId+","+destinationUserId+","+amount+")";
  }

  public String removeMoneyFromSender() {
    return "update user_balances "
        + "set current_balance=current_balance-"+amount+
        " where id="+sourceUserId+" and current_balance>="+amount;
  }

  public String depositMoneyInRecipient() {
    return "update user_balances "
        + "set current_balance=current_balance+"+amount+
        " where id="+destinationUserId;
  }
}
