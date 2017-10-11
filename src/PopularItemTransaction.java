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
public class PopularItemTransaction {
    private PreparedStatement selectLastOrdersStmt;
    private PreparedStatement selectMaxQuantityStmt;
    private PreparedStatement selectPopularItemStmt;
    private PreparedStatement selectOrderWithItemStmt;

    private Session session;
    /* popular items */
    private static final String SELECT_LAST_ORDERS =
            "SELECT o_id, o_c_id, o_entry_d, o_c_first, o_c_middle, o_c_last "
                    + "FROM orders_by_timestamp "
                    + "WHERE o_w_id = ? AND o_d_id = ? "
                    + "LIMIT ?;";
    private static final String SELECT_MAX_QUANTITY =
            "SELECT MAX(ol_quantity) "
                    + "FROM orders_lines "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
    private static final String SELECT_POPULAR_ITEM =
            "SELECT ol_i_id, ol_i_name "
                    + "FROM orders_lines "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_quantity = ?;";
    private static final String SELECT_ORDER_WITH_ITEM =
            "SELECT * "
                    + "FROM orders_lines "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_i_id = ?;";

    PopularItemTransaction(Session session) {
        this.session = session;
        /* popular items */
        this.selectLastOrdersStmt = session.prepare(SELECT_LAST_ORDERS);
        this.selectMaxQuantityStmt = session.prepare(SELECT_MAX_QUANTITY);
        this.selectPopularItemStmt = session.prepare(SELECT_POPULAR_ITEM);
        this.selectOrderWithItemStmt = session.prepare(SELECT_ORDER_WITH_ITEM);
    }

    /* Start of public methods */

    /**
     *
     * @param wId : used for customer identifier
     * @param dID : used for customer identifier
     * @param numOfOrders : number of lastest order to be considered
     */

    void popularItem(int wId, int dId, int numOfOrders) {
        List<Row> lastOrders = selectLastOrders(wId, dId, numOfOrders);
//        List<Row> customers = new List();
        List<int> popularItems = new List();
        for (int i = 0; i < numOfOrders; i++) {
            orderId = lastOrders.get(i).getInt("o_id");
//            cId = lastOrders.get(i)[1];
//            customers.add(getCustomer(wId, dId, cId));
            popularItem = getPopularItem(wId, dId, orderId);
            for (Row item: popularItem) {
                if popularItems.contains(item.getInt("ol_i_id")) {
                    popularItems.add(item.getInt("ol_i_id"));
                }
            }
        }
        int[] percentage = new int[popularItems.size()];
//        String[] itemName = new String[popularItems.size()]
        for (int i = 0; i < popularItems.size(); i++){
            itemId = popularItems.get(i);
//            orderId = lastOrders.get(i)[0];
//            itemName[i] = getItemName(itemId);
            percentage[i] = getPercentage(wId, dId, lastOrders, itemId)
        }
        outputPopularItems(wId, dId, numOfOrders, lastOrders, customers, popularItems, percentage);
    }


    /*  End of public methods */
    /*  popular items */
    private void selectLastOrders(final int wId, final int dId, final int numOfOrders) {
        ResultSet resultSet = session.execute(selectLastOrdersStmt.bind(wId, dId, numOfOrders));
        List<Row> lastOrders = resultSet.all();
        return lastOrders
    }

//    private void getCustomer(final int wId, final int dId, final int cId) {
//        ResultSet resultSet = session.execute(selectCustomerStmt.bind(wId, dId, cId));
//        List<Row> customers = resultSet.all();
//
//        if(!customers.isEmpty()) {
//            return customers.get(0);
//        }
//    }

    private void getPopularItem(final int wId, final int dId, final int orderId) {
        ResultSet resultSet = session.execute(selectMaxQuantityStmt.bind(wId, dId, orderLine));
        List<Row> maxQuantity= (resultSet.all()).get(0).getInt("ol_quantity");

        ResultSet resultSet = session.execute(selectPopularItemStmt.bind(wId, dId, orderLine, maxQuantity);
        List<Row> popularItem = resultSet.all();
        return popularItem;
    }

//    private void getItemName(final int itemId){
//        ResultSet resultSet = session.execute(selectItemName.bind(itemId);
//        List<Row> itemName = resultSet.all();
//        return itemName.get(0);
//    }

    private void getPercentage(inal int wId, final int dId, final List<Row> lastOrders, final int itemId) {
        int count = 0;
        for (int i = 0; i < lastOrders.size(); i++) {
            orderId = lastOrders.get(i).getInt("o_id");
            ResultSet resultSet = session.execute(selectOrderWithItem.bind(wId, dId, orderId, itemId);
            List<Row> result = resultSet.all();
            if (!result.isEmpty()){
                count++;
            }
        }
        return count

    }

    private void outputPopularItems(){

    }

    /*  End of private methods */
}