import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;

import java.util.List;

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
        // Check against top_balance_customer table?
    }

    private void outputPaymentResults(float payment) {

    }

    /*  End of private methods */
}
