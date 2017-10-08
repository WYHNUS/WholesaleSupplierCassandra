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
class Transactions {
    static final String CONTACT_POINT = Setup.CONTACT_POINT;
    static final String KEY_SPACE = Setup.KEY_SPACE;

    private PreparedStatement selectWarehouseStmt;
    private PreparedStatement selectDistrictStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement selectSmallestOrderStmt;
    private PreparedStatement selectOrderLinesOlAmountSumStmt;
    private PreparedStatement updateWarehouseYTDStmt;
    private PreparedStatement updateDistrictYTDStmt;
    private PreparedStatement updateCustomerByPaymentStmt;
    private PreparedStatement updateOrderByCarrierStmt;
    private PreparedStatement updateOrderLineStmt;
    private PreparedStatement updateCustomerByDeliveryStmt;
    private Row targetWarehouse;
    private Row targetDistrict;
    private Row targetCustomer;
    private Session session;

    private static final String SELECT_WAREHOUSE =
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd "
                    + "FROM warehouses "
                    + "WHERE w_id = ?;";
    private static final String SELECT_DISTRICT =
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd "
                    + "FROM districts "
                    + "WHERE d_w_id = ? AND d_id = ?;";
    private static final String SELECT_CUSTOMER =
            "SELECT c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, "
                    + "c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
                    + "c_discount, c_balance, c_ytd_payment, c_payment_cnt "
                    + "FROM customers "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String SELECT_SMALLEST_ORDER =
            "SELECT MIN(o_id), o_c_id "
                    + "FROM orders_by_timestamp "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = 0;";
    private static final String SELECT_ORDER_LINES_OL_AMOUNT_SUM =
            "SELECT SUM(ol_amount) AS ol_amount_sum "
                    + "FROM order_lines "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
    private static final String UPDATE_WAREHOUSE_YTD =
            "UPDATE warehouses "
                    + "SET w_ytd = ? "
                    + "WHERE w_id = ?;";
    private static final String UPDATE_DISTRICT_YTD =
            "UPDATE districts "
                    + "SET d_ytd = ? "
                    + "WHERE d_w_id = ? AND d_id = ?;";
    private static final String UPDATE_CUSTOMER_BY_PAYMENT =
            "UPDATE customers "
                    + "SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String UPDATE_CUSTOMER_BY_DELIVERY =
            "UPDATE customer "
                    + "SET c_balance = ?, c_delivery_cnt = ? "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String UPDATE_ORDER_LINE =
            "UPDATE order_lines "
                    + "SET ol_delivery_d = ? "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_id = ?;";
    private static final String UPDATE_ORDER_BY_CARRIER =
            "UPDATE customers "
                    + "SET o_carrier_id = ? "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;";

    Transactions() {
        Cluster cluster = Cluster.builder()
                .addContactPoint(CONTACT_POINT)
                .build();
        session = cluster.connect(KEY_SPACE);
        this.selectWarehouseStmt = session.prepare(SELECT_WAREHOUSE);
        this.selectDistrictStmt = session.prepare(SELECT_DISTRICT);
        this.selectCustomerStmt = session.prepare(SELECT_CUSTOMER);
        this.selectSmallestOrderStmt = session.prepare(SELECT_SMALLEST_ORDER);
        this.selectOrderLinesOlAmountSumStmt = session.prepare(SELECT_ORDER_LINES_OL_AMOUNT_SUM);
        this.updateWarehouseYTDStmt = session.prepare(UPDATE_WAREHOUSE_YTD);
        this.updateDistrictYTDStmt = session.prepare(UPDATE_DISTRICT_YTD);
        this.updateCustomerByPaymentStmt = session.prepare(UPDATE_CUSTOMER_BY_PAYMENT);
        this.updateOrderByCarrierStmt = session.prepare(UPDATE_ORDER_BY_CARRIER);
        this.updateOrderLineStmt = session.prepare(UPDATE_ORDER_LINE);
        this.updateCustomerByDeliveryStmt = session.prepare(UPDATE_CUSTOMER_BY_DELIVERY);
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

    /**
     *
     * @param wId : used for customer identifier
     * @param dId : used for customer identifier
     * @param cId : used for customer identifier
     * @param payment : payment amount
     */
    void processPayment(int wId, int dId, int cId, float payment) {
        // Update the warehouse C_W_ID by incrementing W_YTD by PAYMENT
        selectWarehouse(wId);
        updateWarehouseYTD(wId, payment);
        // Update the district (C_W_ID,C_D_ID) by incrementing D_YTD by PAYMENT
        selectDistrict(wId, dId);
        updateDistrictYTD(wId, dId, payment);
        // Update the customer (C_W_ID, C_D_ID, C_ID) as follows:
        selectCustomer(wId, dId, cId);
        updateCustomerByPayment(wId, dId, cId, payment);
        outputPaymentResults(payment);
    }

    /**
     *
     * @param wId : used for customer identifier
     * @param carrierId : used for carrier identifier
     */
    void processDelivery(int wId, int carrierId) {
        for (int dId=1; dId<=10; dId++) {
            int[] idPair = selectSmallestOrder(wId, dId);
            if (idPair[0] == -1) {
                return ;
            }
            int oId = idPair[0];
            int cId = idPair[1];
            // May need to include o_entry_d here
            updateOrderByCarrier(wId, dId, oId, carrierId);
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
            String ol_delivery_d = sdf.format(date);
            updateOrderLines(wId, dId, oId, ol_delivery_d);
            double olAmountSum = selectOrderLinesOlAmountSum(wId, dId, oId);
            updateCustomerByDelivery(wId, dId, cId, olAmountSum);
        }
    }

    /*  End of public methods */

    /*  Start of private methods */

    private void selectWarehouse(final int wId) {
        ResultSet resultSet = session.execute(selectWarehouseStmt.bind(wId));
        List<Row> warehouses = resultSet.all();

        if(!warehouses.isEmpty()) {
            targetWarehouse = warehouses.get(0);
        }
    }

    private void selectDistrict(final int w_id, final int d_id) {
        ResultSet resultSet = session.execute(selectDistrictStmt.bind(w_id, d_id));
        List<Row> districts = resultSet.all();

        if(!districts.isEmpty()) {
            targetDistrict = districts.get(0);
        }
    }

    private void selectCustomer(final int w_id, final int d_id, final int c_id) {
        ResultSet resultSet = session.execute(selectCustomerStmt.bind(w_id, d_id, c_id));
        List<Row> customers = resultSet.all();

        if(!customers.isEmpty()) {
            targetCustomer = customers.get(0);
        }
    }

    private int[] selectSmallestOrder(final int w_id, final int d_id) {
        ResultSet resultSet = session.execute(selectSmallestOrderStmt.bind(w_id, d_id));
        List<Row> orders = resultSet.all();
        int[] idPair = new int[2];
        idPair[0] = -1;

        if(!orders.isEmpty()) {
            int oId = orders.get(0).getInt("o_id");
            int cId = orders.get(0).getInt("o_c_id");

            idPair[0] = oId;
            idPair[1] = cId;
            return idPair;
        }

        return idPair;
    }

    private double selectOrderLinesOlAmountSum(final int w_id, final int d_id, final int o_id) {
        ResultSet resultSet = session.execute(selectOrderLinesOlAmountSumStmt.bind(w_id, d_id, o_id));
        List<Row> resultRow = resultSet.all();

        if (!resultRow.isEmpty()) {
            return resultRow.get(0).getDouble("ol_amount_sum");
        }

        return -1;
    }

    private void updateWarehouseYTD(final int w_id, final float payment) {
        double w_ytd = targetWarehouse.getDouble("w_ytd") + payment;
        session.execute(updateWarehouseYTDStmt.bind(w_ytd, w_id));
    }

    private void updateDistrictYTD(final int w_id, final int d_id, final float payment) {
        double d_ytd = targetDistrict.getDouble("d_ytd") + payment;
        session.execute(updateDistrictYTDStmt.bind(d_ytd, w_id, d_id));
    }

    private void updateCustomerByPayment(final int w_id, final int d_id, final int c_id, final float payment) {
        double c_balance = targetCustomer.getDouble("c_balance") - payment;
        float c_ytd_payment = targetCustomer.getFloat("c_ytd_payment") + payment;
        int c_payment_cnt = targetCustomer.getInt("c_payment_cnt") + 1;
        session.execute(updateCustomerByPaymentStmt.bind(c_balance, c_ytd_payment, c_payment_cnt, w_id, d_id, c_id));
    }

    private void updateOrderByCarrier(final int w_id, final int d_id, final int o_id, final int carrier_id) {
        session.execute(updateOrderByCarrierStmt.bind(carrier_id, w_id, d_id, o_id));
    }

    private void updateOrderLines(final int w_id, final int d_id, final int o_id, String ol_delivery_d) {
        session.execute(updateOrderLineStmt.bind(ol_delivery_d, w_id, d_id, o_id));
    }

    private void updateCustomerByDelivery(final int w_id, final int d_id, final int c_id, final double ol_amount_sum) {
        double c_balance = targetCustomer.getDouble("c_balance") + ol_amount_sum;
        int c_delivery_cnt = targetCustomer.getInt("c_delivery_cnt") + 1;
        session.execute(updateCustomerByDeliveryStmt.bind(c_balance, c_delivery_cnt, w_id, d_id, c_id));
    }

    private void outputPaymentResults(float payment) {

    }

    /*  End of private methods */
}
