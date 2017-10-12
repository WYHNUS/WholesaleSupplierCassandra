import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Implementation of seven transaction types:
 * 1. New Order Transaction processes a new customer order.
 * 2. Payment Transaction processes a customer payment for an order.
 * 3. Delivery Transaction processes the delivery of the oldest yet-to-be-delivered order
 *    for each of the 10 districts in a specified warehouse.
 * 4. Order-Status Transaction queries the status of the last order of a specified customer.
 * 5. Stock-Level Transaction checks the stock level of a specified number of last items sold at a warehouse district.
 * 6. Popular-Item Transaction identifies the most popular items sold in each of a specified number of last orders
 *    at a specified warehouse district.
 * 7. Top-Balance Transaction identifies the top-10 customers with the highest outstanding payment balance.
 */
public class TopBalanceTransaction {
    /* popular items */
    private PreparedStatement selectTopBalanceStmt;

    private Session session;

    private static final String SELECT_TOP_BALANCE =
            "SELECT * "
            + "FROM customers_balances "
            + "LIMIT 10;";
    TopBalanceTransaction(Session session) {
        this.session = session;
        this.selectTopBalanceStmt = session.prepare(SELECT_TOP_BALANCE);
    }

    /* Start of public methods */
    void topBalance() {
        ResultSet resultSet = session.execute(selectTopBalance);
        List<Row> topCustomers = resultSet.all();
        outputTopBalance(topCustmers);

    }

    /*  End of public methods */

    private void outputTopBalance(){
    }

    /*  End of private methods */
}
