import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

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
    static final String CONTACT_POINT = Setup.CONTACT_POINT;
    static final String KEY_SPACE = Setup.KEY_SPACE;

    private Session session;
    private PaymentTransaction paymentTransaction;
    private DeliveryTransaction deliveryTransaction;
    private PopularItemTransaction popularItemTransaction;
    //private TopBalanceTransaction topBalanceTransaction;

    Transactions() {
        Cluster cluster = Cluster.builder()
                .addContactPoint(CONTACT_POINT)
                .build();
        session = cluster.connect(KEY_SPACE);
        paymentTransaction = new PaymentTransaction(session);
        deliveryTransaction = new DeliveryTransaction(session);
        popularItemTransaction = new PopularItemTransaction(session) ;
        //topBalanceTransaction = new TopBalanceTransaction(session);
    }

    /* Start of public methods */

    /**
     *
     * @param cId : used for customer identifier
     * @param wId : used for customer identifier
     * @param dID : used for customer identifier
     * @param itemOrders : each item consist of:
     *                   - item number for item
     *                   - supplier warehouse for item
     *                   - quantity ordered for item
     */
    void processOrder(int cId, int wId, int dID, List<List<Integer>> itemOrders) {

    }

    void processPayment(int wId, int dId, int cId, float payment) {
        paymentTransaction.processPayment(wId, dId, cId, payment);
    }

    void processDelivery(int wId, int carrierId) {
        deliveryTransaction.processDelivery(wId, carrierId);
    }

    void popularItem(int wId, int dId, int numOfOrders) {
        popularItemTransaction.popularItem(wId, dId, numOfOrders);
    }

//    void topBalance() {
//        topBalanceTransaction.topBalance();
//    }
}
