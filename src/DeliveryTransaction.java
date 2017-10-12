import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DeliveryTransaction {
    private PreparedStatement selectSmallestOrderStmt;
    private PreparedStatement selectOrderLinesOlAmountSumStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement updateOrderByCarrierStmt;
    private PreparedStatement updateOrderLineStmt;
    private PreparedStatement updateCustomerByDeliveryStmt;
    private Row targetWarehouse;
    private Row targetDistrict;
    private Row targetCustomer;
    private Session session;

    private static final String SELECT_SMALLEST_ORDER =
            "SELECT o_id, o_c_id "
                    + "FROM orders_by_id "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = 0 LIMIT 1;";
    private static final String SELECT_ORDER_LINES_OL_AMOUNT_SUM =
            "SELECT SUM(ol_amount) AS ol_amount_sum "
                    + "FROM order_lines "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
    private static final String SELECT_CUSTOMER =
            "SELECT c_balance, c_delivery_cnt "
                    + "FROM customers "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String UPDATE_CUSTOMER_BY_DELIVERY =
            "UPDATE customer "
                    + "SET c_balance = ?, c_delivery_cnt = ?, c_carrier_id = ?"
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String UPDATE_ORDER_LINE =
            "UPDATE order_lines "
                    + "SET ol_delivery_d = ? "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_id = ?;";
    private static final String UPDATE_ORDER_BY_CARRIER =
            "UPDATE customers "
                    + "SET o_carrier_id = ? "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;";

    DeliveryTransaction(Session session) {
        this.session = session;
        this.selectSmallestOrderStmt = session.prepare(SELECT_SMALLEST_ORDER);
        this.selectOrderLinesOlAmountSumStmt = session.prepare(SELECT_ORDER_LINES_OL_AMOUNT_SUM);
        this.selectCustomerStmt = session.prepare(SELECT_CUSTOMER);
        this.updateOrderByCarrierStmt = session.prepare(UPDATE_ORDER_BY_CARRIER);
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
            int[] idPair = selectSmallestOrder(wId, dId);
            if (idPair[0] == -1) {
                return ;
            }
            int oId = idPair[0];
            int cId = idPair[1];
            // May need to include o_entry_d here
            updateOrderByCarrier(wId, dId, oId, carrierId);
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String ol_delivery_d = sdf.format(date);
            updateOrderLines(wId, dId, oId, ol_delivery_d);
            selectCustomer(wId, dId, cId);
            double olAmountSum = selectOrderLinesOlAmountSum(wId, dId, oId);
            updateCustomerByDelivery(wId, dId, cId, olAmountSum, carrierId);
        }
    }

    /*  End of public methods */

    /*  Start of private methods */

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

    private void selectCustomer(final int w_id, final int d_id, final int c_id) {
        ResultSet resultSet = session.execute(selectCustomerStmt.bind(w_id, d_id, c_id));
        List<Row> customers = resultSet.all();

        if(!customers.isEmpty()) {
            targetCustomer = customers.get(0);
        }
    }

    private void updateOrderByCarrier(final int w_id, final int d_id, final int o_id, final int carrier_id) {
        session.execute(updateOrderByCarrierStmt.bind(carrier_id, w_id, d_id, o_id));
    }

    private void updateOrderLines(final int w_id, final int d_id, final int o_id, String ol_delivery_d) {
        session.execute(updateOrderLineStmt.bind(ol_delivery_d, w_id, d_id, o_id));
    }

    private void updateCustomerByDelivery(final int w_id, final int d_id, final int c_id, final double ol_amount_sum, final int carrier_id) {
        double c_balance = targetCustomer.getDouble("c_balance") + ol_amount_sum;
        int c_delivery_cnt = targetCustomer.getInt("c_delivery_cnt") + 1;
        session.execute(updateCustomerByDeliveryStmt.bind(c_balance, c_delivery_cnt, carrier_id, w_id, d_id, c_id));
    }

    /*  End of private methods */
}
