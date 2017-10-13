package main.java;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DeliveryTransaction {
    private PreparedStatement selectSmallestOrderStmt;
    private PreparedStatement selectOrderLinesStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement updateOrderByIdStmt;
    private PreparedStatement updateOrderByTimestampStmt;
    private PreparedStatement updateOrderLineStmt;
    private PreparedStatement updateCustomerByDeliveryStmt;
    private Row targetCustomer;
    private Row targetOrder;
    private Session session;

    private static final String SELECT_SMALLEST_ORDER =
            "SELECT o_id, o_c_id, o_entry_d "
                    + "FROM orders_by_id "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = -1 LIMIT 1 ALLOW FILTERING;";
    private static final String SELECT_ORDER_LINES =
            "SELECT ol_number, ol_amount "
                    + "FROM order_lines "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
    private static final String SELECT_CUSTOMER =
            "SELECT c_balance, c_delivery_cnt "
                    + "FROM customers "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String UPDATE_CUSTOMER_BY_DELIVERY =
            "UPDATE customers "
                    + "SET c_balance = ?, c_delivery_cnt = ?, c_carrier_id = ?"
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String UPDATE_ORDER_LINE =
            "UPDATE order_lines "
                    + "SET ol_delivery_d = ? "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?;";
    private static final String UPDATE_ORDER_BY_ID =
            "UPDATE orders_by_id "
                    + "SET o_carrier_id = ? "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? AND o_c_id = ?;";
    private static final String UPDATE_ORDER_BY_TIMESTAMP =
            "UPDATE orders_by_timestamp "
                    + "SET o_carrier_id = ? "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_entry_d = ? AND o_id = ? AND o_c_id = ?;";

    DeliveryTransaction(Session session) {
        this.session = session;
        this.selectSmallestOrderStmt = session.prepare(SELECT_SMALLEST_ORDER);
        this.selectOrderLinesStmt = session.prepare(SELECT_ORDER_LINES);
        this.selectCustomerStmt = session.prepare(SELECT_CUSTOMER);
        this.updateOrderByIdStmt = session.prepare(UPDATE_ORDER_BY_ID);
        this.updateOrderByTimestampStmt = session.prepare(UPDATE_ORDER_BY_TIMESTAMP);
        this.updateOrderLineStmt = session.prepare(UPDATE_ORDER_LINE);
        this.updateCustomerByDeliveryStmt = session.prepare(UPDATE_CUSTOMER_BY_DELIVERY);
    }

    /* Start of public methods */

    /**
     *
     * @param wId : used for customer identifier
     * @param carrierId : used for carrier identifier
     */
    void processDelivery(int wId, int carrierId) {
        for (int dId=1; dId<=10; dId++) {
            selectSmallestOrder(wId, dId);
            int oId = targetOrder.getInt("o_id");
            int cId = targetOrder.getInt("o_c_id");
            Date o_entry_d = targetOrder.getTimestamp("o_entry_d");
            updateOrderByCarrier(wId, dId, cId, oId, carrierId, o_entry_d);

            Date ol_delivery_d = new Date();
            BigDecimal olAmountSum = selectAndUpdateOrderLines(wId, dId, oId, ol_delivery_d);
            selectCustomer(wId, dId, cId);
            updateCustomerByDelivery(wId, dId, cId, olAmountSum, carrierId);
        }
    }

    /*  End of public methods */

    /*  Start of private methods */

    private void selectSmallestOrder(final int w_id, final int d_id) {
        ResultSet resultSet = session.execute(selectSmallestOrderStmt.bind(w_id, d_id));
        List<Row> orders = resultSet.all();

        if(!orders.isEmpty()) {
            targetOrder = orders.get(0);
        }
    }

    private BigDecimal selectAndUpdateOrderLines(final int w_id, final int d_id, final int o_id, final Date ol_delivery_d) {
        ResultSet resultSet = session.execute(selectOrderLinesStmt.bind(w_id, d_id, o_id));
        List<Row> resultRow = resultSet.all();
        BigDecimal sum = new BigDecimal(0);

        for (int i=0; i<resultRow.size(); i++) {
            Row orderLine = resultRow.get(i);
            BigDecimal partial_sum = orderLine.getDecimal("ol_amount");
            int ol_number = orderLine.getInt("ol_number");

            sum = sum.add(partial_sum);
            session.execute(updateOrderLineStmt.bind(ol_delivery_d, w_id, d_id, o_id, ol_number));
        }

        return sum;
    }

    private void selectCustomer(final int w_id, final int d_id, final int c_id) {
        ResultSet resultSet = session.execute(selectCustomerStmt.bind(w_id, d_id, c_id));
        List<Row> customers = resultSet.all();

        if(!customers.isEmpty()) {
            targetCustomer = customers.get(0);
        }
    }

    private void updateOrderByCarrier(final int w_id, final int d_id, final int c_id, final int o_id, final int carrier_id, final Date o_entry_d) {
        session.execute(updateOrderByIdStmt.bind(carrier_id, w_id, d_id, o_id, c_id));
        session.execute(updateOrderByTimestampStmt.bind(carrier_id, w_id, d_id, o_entry_d, o_id, c_id));
    }

    private void updateCustomerByDelivery(final int w_id, final int d_id, final int c_id, final BigDecimal ol_amount_sum, final int carrier_id) {
        BigDecimal c_balance = targetCustomer.getDecimal("c_balance").add(ol_amount_sum);
        int c_delivery_cnt = targetCustomer.getInt("c_delivery_cnt") + 1;
        session.execute(updateCustomerByDeliveryStmt.bind(c_balance, c_delivery_cnt, carrier_id, w_id, d_id, c_id));
    }

    /*  End of private methods */
}
