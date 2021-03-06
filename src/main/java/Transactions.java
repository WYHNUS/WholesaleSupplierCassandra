package main.java;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.QueryOptions;

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
class Transactions {
    static final String[] CONTACT_POINTS = Setup.CONTACT_POINTS;
    static final String KEY_SPACE = Setup.KEY_SPACE;

    private Session session;
    private OrderTransaction orderTransaction;
    private PaymentTransaction paymentTransaction;
    private DeliveryTransaction deliveryTransaction;
    private OrderStatusTransaction orderStatusTransaction;
    private StockLevelTransaction stockLevelTransaction;
    private PopularItemTransaction popularItemTransaction;
    private TopBalanceTransaction topBalanceTransaction;

    Transactions(int index, String consistencyLevel) {
        QueryOptions queryOptions;
        if (consistencyLevel.equalsIgnoreCase("ONE")) {
            queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        } else {
            queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM);
        }

        Cluster cluster = Cluster.builder()
                .addContactPoint(CONTACT_POINTS[index % 5])
                .withQueryOptions(queryOptions)
                .build();
        session = cluster.connect(KEY_SPACE);

        orderTransaction = new OrderTransaction(session);
        paymentTransaction = new PaymentTransaction(session);
        deliveryTransaction = new DeliveryTransaction(session);
        orderStatusTransaction = new OrderStatusTransaction(session);
        stockLevelTransaction = new StockLevelTransaction(session);
        popularItemTransaction = new PopularItemTransaction(session) ;
        topBalanceTransaction = new TopBalanceTransaction(session);
    }

    /* Start of public methods */

    void processOrder(int cId, int wId, int dID, List<List<Integer>> itemOrders) {
        orderTransaction.processOrder(cId, wId, dID, itemOrders);
    }

    void processPayment(int wId, int dId, int cId, float payment) {
        paymentTransaction.processPayment(wId, dId, cId, payment);
    }

    void processDelivery(int wId, int carrierId) {
        deliveryTransaction.processDelivery(wId, carrierId);
    }

    void processOrderStatus(int c_W_ID, int c_D_ID, int c_ID){
        orderStatusTransaction.processOrderStatus(c_W_ID, c_D_ID, c_ID);
    }

    void processStockLevel(int w_ID, int d_ID, int T, int L){
        stockLevelTransaction.processStockLevel(w_ID, d_ID, T, L);
    }

    void popularItem(int wId, int dId, int numOfOrders) {
        popularItemTransaction.popularItem(wId, dId, numOfOrders);
    }

    void topBalance() { topBalanceTransaction.topBalance(); }
}
