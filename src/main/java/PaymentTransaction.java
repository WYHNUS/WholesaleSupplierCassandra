package main.java;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;

import java.util.List;
import java.math.BigDecimal;

public class PaymentTransaction {
    private PreparedStatement selectWarehouseStmt;
    private PreparedStatement selectDistrictStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement updateWarehouseYTDStmt;
    private PreparedStatement updateDistrictYTDStmt;
    private PreparedStatement updateCustomerByPaymentStmt;
    private Row targetWarehouse;
    private Row targetDistrict;
    private Row targetCustomer;
    private Session session;

    private static final String MESSAGE_WAREHOUSE = "Warehouse address: Street(%1$s %2$s) City(%3$s) State(%4$s) Zip(%5$s)";
    private static final String MESSAGE_DISTRICT = "District address: Street(%1$s %2$s) City(%3$s) State(%4$s) Zip(%5$s)";
    private static final String MESSAGE_CUSTOMER = "Customer: Identifier(%1$s, %2$s, %3$s), Name(%4$s, %5$s, %6$s), "
            + "Address(%7$s, %8$s, %9$s, %10$s, %11$s), Phone(%12$s), Since(%13$s), Credits(%14$s, %15$s, %16$s, %17$s)";
    private static final String MESSAGE_PAYMENT = "Payment amount: %1$s";
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

    PaymentTransaction(Session session) {
        this.session = session;
        this.selectWarehouseStmt = session.prepare(SELECT_WAREHOUSE);
        this.selectDistrictStmt = session.prepare(SELECT_DISTRICT);
        this.selectCustomerStmt = session.prepare(SELECT_CUSTOMER);
        this.updateWarehouseYTDStmt = session.prepare(UPDATE_WAREHOUSE_YTD);
        this.updateDistrictYTDStmt = session.prepare(UPDATE_DISTRICT_YTD);
        this.updateCustomerByPaymentStmt = session.prepare(UPDATE_CUSTOMER_BY_PAYMENT);
    }

    /* Start of public methods */
    /**
     *
     * @param wId : used for customer identifier
     * @param dId : used for customer identifier
     * @param cId : used for customer identifier
     * @param payment : payment amount
     */
    void processPayment(int wId, int dId, int cId, float payment) {
        BigDecimal payment_decimal = new BigDecimal(payment);

        // Update the warehouse C_W_ID by incrementing W_YTD by PAYMENT
        selectWarehouse(wId);
        updateWarehouseYTD(wId, payment_decimal);
        // Update the district (C_W_ID,C_D_ID) by incrementing D_YTD by PAYMENT
        selectDistrict(wId, dId);
        updateDistrictYTD(wId, dId, payment_decimal);
        // Update the customer (C_W_ID, C_D_ID, C_ID) as follows:
        selectCustomer(wId, dId, cId);
        updateCustomerByPayment(wId, dId, cId, payment_decimal);
        outputPaymentResults(payment);
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

    private void updateWarehouseYTD(final int w_id, final BigDecimal payment) {
        BigDecimal w_ytd = targetWarehouse.getDecimal("w_ytd").add(payment);
        session.execute(updateWarehouseYTDStmt.bind(w_ytd, w_id));
    }

    private void updateDistrictYTD(final int w_id, final int d_id, final BigDecimal payment) {
        BigDecimal d_ytd = targetDistrict.getDecimal("d_ytd").add(payment);
        session.execute(updateDistrictYTDStmt.bind(d_ytd, w_id, d_id));
    }

    private void updateCustomerByPayment(final int w_id, final int d_id, final int c_id, final BigDecimal payment) {
        BigDecimal c_balance = targetCustomer.getDecimal("c_balance").subtract(payment);
        float c_ytd_payment = targetCustomer.getFloat("c_ytd_payment") + payment.floatValue();
        int c_payment_cnt = targetCustomer.getInt("c_payment_cnt") + 1;
        session.execute(updateCustomerByPaymentStmt.bind(c_balance, c_ytd_payment, c_payment_cnt, w_id, d_id, c_id));
    }

    private void outputPaymentResults(float payment) {
        System.out.println(String.format(MESSAGE_WAREHOUSE,
                targetWarehouse.getString("w_street_1"),
                targetWarehouse.getString("w_street_2"),
                targetWarehouse.getString("w_city"),
                targetWarehouse.getString("w_state"),
                targetWarehouse.getString("w_zip")));

        System.out.println(String.format(MESSAGE_DISTRICT,
                targetDistrict.getString("d_street_1"),
                targetDistrict.getString("d_street_2"),
                targetDistrict.getString("d_city"),
                targetDistrict.getString("d_state"),
                targetDistrict.getString("d_zip")));

        System.out.println(String.format(MESSAGE_CUSTOMER,
                targetCustomer.getInt("c_w_id"),
                targetCustomer.getInt("c_d_id"),
                targetCustomer.getInt("c_id"),

                targetCustomer.getString("c_first"),
                targetCustomer.getString("c_middle"),
                targetCustomer.getString("c_last"),

                targetCustomer.getString("c_street_1"),
                targetCustomer.getString("c_street_2"),
                targetCustomer.getString("c_city"),
                targetCustomer.getString("c_state"),
                targetCustomer.getString("c_zip"),

                targetCustomer.getString("c_phone"),
                targetCustomer.getTimestamp("c_since"),

                targetCustomer.getString("c_credit"),
                targetCustomer.getDecimal("c_credit_lim"),
                targetCustomer.getDecimal("c_discount"),
                targetCustomer.getDecimal("c_balance")));

        System.out.println(String.format(MESSAGE_PAYMENT, payment));
    }

    /*  End of private methods */
}
