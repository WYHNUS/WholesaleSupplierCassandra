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
        selectLastOrdersStmt = session.prepare(SELECT_LAST_ORDERS);
        selectMaxQuantityStmt = session.prepare(SELECT_MAX_QUANTITY);
        selectPopularItemStmt = session.prepare(SELECT_POPULAR_ITEM);
        selectOrderWithItemStmt = session.prepare(SELECT_ORDER_WITH_ITEM);
    }

    /* Start of public methods */

    /**
     *
     * @param wId : used for customer identifier
     * @param dId : used for customer identifier
     * @param numOfOrders : number of lastest order to be considered
     */

    void popularItem(int wId, int dId, int numOfOrders) {
        List<Row> lastOrders = selectLastOrders(wId, dId, numOfOrders);
        int num = lastOrders.size();
//        List<Row> customers = new List();
        List<Integer> popularItems = new ArrayList();
        System.out.println("order ids");
        for (int i = 0; i < num; i++) {
            int orderId = lastOrders.get(i).getInt("o_id");
            System.out.println(orderId);
//            cId = lastOrders.get(i)[1];
//            customers.add(getCustomer(wId, dId, cId));
            List<Row>popularItem = getPopularItem(wId, dId, orderId);
            System.out.println("item ids:");
            for (Row item: popularItem) {
                int itemId = item.getInt("ol_i_id");
                System.out.println(itemId);
                if (popularItems.contains(itemId)) {
                    popularItems.add(itemId);
                }
            }
        }
        int[] percentage = new int[popularItems.size()];
//        String[] itemName = new String[popularItems.size()]
        System.out.println("percentage:");
        for (int i = 0; i < popularItems.size(); i++){
            int itemId = popularItems.get(i);
//            orderId = lastOrders.get(i)[0];
//            itemName[i] = getItemName(itemId);
            percentage[i] = getPercentage(wId, dId, lastOrders, itemId);
        }
        //outputPopularItems(wId, dId, numOfOrders, lastOrders,popularItems, percentage);
    }


    /*  End of public methods */
    /*  popular items */
    private List<Row> selectLastOrders(final int wId, final int dId, final int numOfOrders) {
        ResultSet resultSet = session.execute(selectLastOrdersStmt.bind(wId, dId, numOfOrders));
        List<Row> lastOrders = resultSet.all();
        return lastOrders;
    }

//    private void getCustomer(final int wId, final int dId, final int cId) {
//        ResultSet resultSet = session.execute(selectCustomerStmt.bind(wId, dId, cId));
//        List<Row> customers = resultSet.all();
//
//        if(!customers.isEmpty()) {
//            return customers.get(0);
//        }
//    }

    private List<Row> getPopularItem(final int wId, final int dId, final int orderId) {
        ResultSet resultSet1 = session.execute(selectMaxQuantityStmt.bind(wId, dId, orderId));
        int maxQuantity= (resultSet1.all()).get(0).getInt("ol_quantity");

        ResultSet resultSet2 = session.execute(selectPopularItemStmt.bind(wId, dId, orderId, maxQuantity));
        List<Row> popularItem = resultSet2.all();
        return popularItem;
    }

//    private void getItemName(final int itemId){
//        ResultSet resultSet = session.execute(selectItemName.bind(itemId);
//        List<Row> itemName = resultSet.all();
//        return itemName.get(0);
//    }

    private int getPercentage(final int wId, final int dId, final List<Row> lastOrders, final int itemId) {
        int count = 0;
        for (int i = 0; i < lastOrders.size(); i++) {
            int orderId = lastOrders.get(i).getInt("o_id");
            ResultSet resultSet = session.execute(selectOrderWithItemStmt.bind(wId, dId, orderId, itemId));
            List<Row> result = resultSet.all();
            if (!result.isEmpty()){
                count++;
            }
        }
        System.out.println(count);
        return count;

    }

    private void outputPopularItems(){

    }

    /*  End of private methods */
}
