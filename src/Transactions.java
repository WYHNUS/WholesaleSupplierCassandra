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
    private OrderStatusTransaction orderStatusTransaction;
    private StockLevelTransaction stockLevelTransaction;

    Transactions() {
        Cluster cluster = Cluster.builder()
                .addContactPoint(CONTACT_POINT)
                .build();
        session = cluster.connect(KEY_SPACE);
        paymentTransaction = new PaymentTransaction(session);
        deliveryTransaction = new DeliveryTransaction(session);
        orderStatusTransaction = new OrderStatusTransaction(session);
        stockLevelTransaction = new StockLevelTransaction(session);
    }

    /* Start of public methods */

    /**
     * New Order Transaction
     * @param cId : used for customer identifier
     * @param wId : used for customer identifier
     * @param dID : used for customer identifier
     * @param itemOrders : each itemOrder consist of:
     *                   - itemId: item number for item
     *                   - warehouseId: supplier warehouse for item
     *                   - quantity: quantity ordered for item
     */
    void processOrder(int cId, int wId, int dID, List<List<Integer>> itemOrders) {
        // query customers using (wId, dID, cId)
        // query districts using (wId, dID) to get D_TAX
        // query warehouses using (wId) to get W_TAX

        // read N=D_NEXT_O_ID from districts using (wId, dID)

        // update district (wId, dID) by increase D_NEXT_O_ID by one

        // let O_ENTRY_D = current date and time
        // let O_OL_CNT = itemOrders.size()
        // Check if any of the supplier warehouse for item != wId, if exists, O_ALL_LOCAL = 0, else O_ALL_LOCAL = 1
        // create new order (O_ID=N, dID, wId, cId, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)

        // TOTOAL_AMOUNT = 0
        // for each itemOrder with index i in itemOrders
            // query stocks, use (warehouseId, itemId) to get S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT
            // set ADJUSTED_QTY = S_QUANTITY - quantity
            // if ADJUSTED_QTY < 10, ADJUSTED_QTY += 100
            // S_YTD += quantity
            // S_ORDER_CNT++
            // if (warehouseId != wId) S_REMOTE_CNT++
            // update stock (warehouseId, itemId) with:
                // (ADJUSTED_QTY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT)
            // query items using itemId to get I_PRICE, itemName
            // ITEM_AMOUNT = quantity * I_PRICE
            // TOTOAL_AMOUNT += ITEM_AMOUNT
            // create new order-line with (OL_O_ID = N, OL_D_ID = dID, OL_W_ID = wId,
            // OL_NUMBER = i, OL_I_ID = itemId, OL_SUPPLY_W_ID = warehouseId, OL_QUANTITY = quantity,
            // OL_AMOUNT = ITEM_AMOUNT, OL_DIST_INFO = "S_DIST"+dID)

        // TOTOAL_AMOUNT *= (1 + D_TAX + W_TAX) * (1 - C_DISCOUNT)

        // output customer identifier (wId, dID, cId), lastname C_LAST, credit C_CREDIT, discount C_DISCOUNT
        // output W_TAX and D_TAX
        // output O_ID, O_ENTRY_D
        // output itemOrders.size(), TOTOAL_AMOUNT
        // for each itemOrder
            // output itemId, itemName, warehouseId, quantity, OL_AMOUNT, S_QUANTITY
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
}
