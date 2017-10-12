import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

class OrderTransaction {
    private Session session;
    private PreparedStatement selectWarehouseStmt;
    private PreparedStatement selectDistrictStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement updateDistrictNextOIdStmt;

    private static final String SELECT_WAREHOUSE =
            " SELECT W_TAX FROM warehouses "
                    + " WHERE W_ID = ?; ";
    private static final String SELECT_DISTRICT =
            " SELECT D_TAX, D_NEXT_O_ID FROM districts "
                    + " WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String SELECT_CUSTOMER =
            "SELECT c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, "
                    + "c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
                    + "c_discount, c_balance, c_ytd_payment, c_payment_cnt "
                    + "FROM customers "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String UPDATE_DISTRICT_NEXT_O_ID =
            "UPDATE districts "
                    + " SET D_NEXT_O_ID = ? "
                    + " WHERE D_W_ID = ? AND D_ID = ?; ";

    OrderTransaction(Session session) {
        this.session = session;
        this.selectWarehouseStmt = session.prepare(SELECT_WAREHOUSE);
        this.selectDistrictStmt = session.prepare(SELECT_DISTRICT);
        this.selectCustomerStmt = session.prepare(SELECT_CUSTOMER);
        this.updateDistrictNextOIdStmt = session.prepare(UPDATE_DISTRICT_NEXT_O_ID);
    }

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
        Row customer = getCustomer(wId, dID, cId);
        Row district = getDTax(wId, dID);
        Row warehouse = getWTax(wId);

        double dTax = district.getDecimal("D_TAX").doubleValue();
        double wTax = warehouse.getDecimal("W_TAX").doubleValue();

        // read N=D_NEXT_O_ID from districts using (wId, dID)
        int nextOId = district.getInt("D_NEXT_O_ID");

        // update district (wId, dID) by increase D_NEXT_O_ID by one
        updateDistrictNextOId(nextOId + 1, wId, dID);

        // let O_ENTRY_D = current date and time
        // let O_OL_CNT = itemOrders.size()
        // Check if any of the supplier warehouse for item != wId, if exists, O_ALL_LOCAL = 0, else O_ALL_LOCAL = 1
        // create new order (O_ID=N, dID, wId, cId, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
        // todo(wangyanhao): create orders_by_id table

        // todo(wangyanhao): update customer with C_LAST_ORDER = O_ID, C_ENTRY_D = O_ENTRY_D

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

    private void updateDistrictNextOId(int nextOId, int wId, int dId) {
        session.execute(updateDistrictNextOIdStmt.bind(nextOId, wId, dId));
    }

    private Row getCustomer(int wId, int dID, int cId) {
        ResultSet resultSet = session.execute(selectCustomerStmt.bind(wId, dID, cId));
        List<Row> customers = resultSet.all();
        return (!customers.isEmpty()) ? customers.get(0) : null;
    }

    private Row getWTax(int wId) {
        ResultSet resultSet = session.execute(selectWarehouseStmt.bind(wId));
        List<Row> warehouses = resultSet.all();
        return (!warehouses.isEmpty()) ? warehouses.get(0) : null;
    }

    private Row getDTax(int wId, int dId) {
        ResultSet resultSet = session.execute(selectDistrictStmt.bind(wId, dId));
        List<Row> districts = resultSet.all();
        return (!districts.isEmpty()) ? districts.get(0) : null;
    }
}
