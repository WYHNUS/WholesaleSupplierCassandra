package main.java;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;
import java.util.ArrayList;

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
    private PreparedStatement selectCustomerNameStmt;

    private Session session;

    private static final String SELECT_TOP_BALANCE =
            "SELECT * "
                    + "FROM customers_balances "
                    + "LIMIT 10;";
    private static final String SELECT_CUSTOMER_NAME =
            "SELECT c_first, c_middle, c_last "
                    + "FROM customers "
                    + "WHERE c_w_id=? AND c_d_id=? AND c_id = ?;";
    TopBalanceTransaction(Session session) {
        this.session = session;
        selectTopBalanceStmt = session.prepare(SELECT_TOP_BALANCE);
        selectCustomerNameStmt = session.prepare(SELECT_CUSTOMER_NAME);
    }

    /* Start of public methods */
    void topBalance() {
        ResultSet resultSet = session.execute(selectTopBalanceStmt.bind());
        List<Row> topCustomers = resultSet.all();
        List<Row> customerNames = new ArrayList();
        for(Row cus: topCustomers){
            ResultSet customerName = session.execute(selectCustomerNameStmt.bind(cus.getInt("c_w_id"),
                    cus.getInt("c_d_id"), cus.getInt("c_id")));
            Row cusN = (customerName.all()).get(0);
            customerNames.add(cusN);
        }
        outputTopBalance(topCustomers, customerNames);
    }

    /*  End of public methods */

    private void outputTopBalance(List<Row> topCustomers, List<Row> customerNames){
        for (int i=0; i<10; i++){
            Row cusName = customerNames.get(i);
            System.out.println("customer name: " + cusName.getString("c_first") + " "
                    + cusName.getString("c_middle") + " " + cusName.getString("c_last"));
            Row cus = topCustomers.get(i);
            System.out.println("customer balance: " + (cus.getDecimal("c_balance")).intValue());
            System.out.println("WId: " + cus.getInt("c_w_id") + " DId: " + cus.getInt("c_d_id"));
        }
    }

    /*  End of private methods */
}
