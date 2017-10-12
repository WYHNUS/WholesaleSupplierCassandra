import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.Date;
import java.util.List;

class OrderTransaction {
    private Session session;
    private PreparedStatement selectWarehouseStmt;
    private PreparedStatement selectDistrictStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement selectStockStmt;
    private PreparedStatement selectItemStmt;
    private PreparedStatement updateDistrictNextOIdStmt;
    private PreparedStatement updateCustomerOrderStmt;
    private PreparedStatement updateStockStmt;
    private PreparedStatement insertOrderByTimestampStmt;
    private PreparedStatement insertOrderByIdStmt;

    private static final String SELECT_WAREHOUSE =
            " SELECT W_TAX "
                    + " FROM warehouses "
                    + " WHERE W_ID = ?; ";
    private static final String SELECT_DISTRICT =
            " SELECT D_TAX, D_NEXT_O_ID "
                    + " FROM districts "
                    + " WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String SELECT_CUSTOMER =
            " SELECT C_FIRST, C_MIDDLE, C_LAST, "
                    + " c_credit, c_credit_lim, "
                    + "c_discount, c_balance, c_ytd_payment, c_payment_cnt "
                    + " FROM customers "
                    + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?; ";
    private static final String SELECT_STOCK =
            " SELECT S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT "
                    + " FROM stocks "
                    + " WHERE S_W_ID = ? AND S_I_ID = ?; ";
    private static final String SELECT_ITEM =
            " SELECT I_NAME, I_PRICE "
                    + " FROM items "
                    + " WHERE I_ID = ?; ";
    private static final String UPDATE_DISTRICT_NEXT_O_ID =
            " UPDATE districts "
                    + " SET D_NEXT_O_ID = ? "
                    + " WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String UPDATE_CUSTOMER_ORDER =
            " UPDATE customers "
                    + " SET C_LAST_ORDER = ? AND C_ENTRY_D = ? AND C_CARRIER_ID = ? "
                    + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?; ";
    private static final String UPDATE_STOCK =
            " UPDATE stocks "
                    + " SET S_QUANTITY = ? AND S_YTD = ? AND S_ORDER_CNT = ? AND S_REMOTE_CNT = ? "
                    + " WHERE S_W_ID = ? AND S_I_ID = ?; ";
    private static final String INSERT_ORDER_BY_TIMESTAMP =
            " INSERT INTO orders_by_timestamp ("
                    + " O_W_ID, O_D_ID, O_ENTRY_D, O_ID, "
                    + " O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, "
                    + " O_C_FIRST, O_C_MIDDLE, O_C_LAST ) "
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
    private static final String INSERT_ORDERS_BY_ID =
            "INSERT INTO orders_by_id ("
                    + " O_W_ID, O_D_ID, O_ID, O_C_ID, "
                    + " O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, "
                    + " O_C_FIRST, O_C_MIDDLE, O_C_LAST ) "
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";

    OrderTransaction(Session session) {
        this.session = session;
        this.selectWarehouseStmt = session.prepare(SELECT_WAREHOUSE);
        this.selectDistrictStmt = session.prepare(SELECT_DISTRICT);
        this.selectCustomerStmt = session.prepare(SELECT_CUSTOMER);
        this.selectStockStmt = session.prepare(SELECT_STOCK);
        this.selectItemStmt = session.prepare(SELECT_ITEM);
        this.updateDistrictNextOIdStmt = session.prepare(UPDATE_DISTRICT_NEXT_O_ID);
        this.updateCustomerOrderStmt = session.prepare(UPDATE_CUSTOMER_ORDER);
        this.updateStockStmt = session.prepare(UPDATE_STOCK);
        this.insertOrderByTimestampStmt = session.prepare(INSERT_ORDER_BY_TIMESTAMP);
        this.insertOrderByIdStmt = session.prepare(INSERT_ORDERS_BY_ID);
    }

    /**
     * New Order Transaction
     * @param cId : used for customer identifier
     * @param wId : used for customer identifier
     * @param dId : used for customer identifier
     * @param itemOrders : each itemOrder consist of:
     *                   - itemId: item number for item
     *                   - warehouseId: supplier warehouse for item
     *                   - quantity: quantity ordered for item
     */
    void processOrder(int cId, int wId, int dId, List<List<Integer>> itemOrders) {
        Row customer = getCustomer(wId, dId, cId);
        Row district = getDTax(wId, dId);
        Row warehouse = getWTax(wId);

        double dTax = district.getDecimal("D_TAX").doubleValue();
        double wTax = warehouse.getDecimal("W_TAX").doubleValue();

        int nextOId = district.getInt("D_NEXT_O_ID");
        updateDistrictNextOId(nextOId + 1, wId, dId);

        // let O_ENTRY_D = current date and time
        Date curDate = new Date();
        int olCount = itemOrders.size();
        int allLocal = 1;
        for (List<Integer> order : itemOrders) {
            if (order.get(1) != wId) {
                allLocal = 0;
            }
        }

        createNewOrder(nextOId, dId, wId, cId, curDate, olCount, allLocal,
                customer.getString("C_FIRST"), customer.getString("C_MIDDLE"), customer.getString("C_LAST"));
        updateCustomerOrder(nextOId, curDate, wId, dId, cId);

        double totalAmount= 0;
        for (int i = 0; i < itemOrders.size(); i++) {
            int iId = itemOrders.get(0).get(0);
            int iWId = itemOrders.get(0).get(1);
            int quantity = itemOrders.get(0).get(2);

            Row stock = selectStock(iWId, iId);

            double adjQuantity = stock.getDecimal("S_QUANTITY").doubleValue() - quantity;
            while (adjQuantity < 10) {
                adjQuantity += 100;
            }

            updateStock(iWId, iId, adjQuantity,
                    stock.getDecimal("S_YTD").doubleValue() + quantity,
                    stock.getInt("S_ORDER_CNT") + 1,
                    (iWId != wId)
                            ? stock.getInt("S_REMOTE_CNT") + 1
                            : stock.getInt("S_REMOTE_CNT"));

            Row item = selectItem(iId);
            long itemAmount = quantity * item.getInt("I_PRICE");

            totalAmount += itemAmount;
            // create new order-line with (OL_O_ID = N, OL_D_ID = dID, OL_W_ID = wId,
            // OL_NUMBER = i, OL_I_ID = itemId, OL_SUPPLY_W_ID = warehouseId, OL_QUANTITY = quantity,
            // OL_AMOUNT = ITEM_AMOUNT, OL_DIST_INFO = "S_DIST"+dID)
//            createNewOrderLine(wId, dId, nextOId, i, iId, item.getString("I_NAME"),
//                    /*delivery date*/ itemAmount, iWId, quantity, "S_DIST" + dId);

        }

        totalAmount = totalAmount * (1 + dTax + wTax) * (1 - customer.getDecimal("C_DISCOUNT").doubleValue());

        // output customer identifier (wId, dID, cId), lastname C_LAST, credit C_CREDIT, discount C_DISCOUNT
        // output W_TAX and D_TAX
        // output O_ID, O_ENTRY_D
        // output itemOrders.size(), TOTOAL_AMOUNT
        // for each itemOrder
        // output itemId, itemName, warehouseId, quantity, OL_AMOUNT, S_QUANTITY
    }

    private Row selectItem(int iId) {
        ResultSet resultSet = session.execute(selectItemStmt.bind(iId));
        List<Row> items = resultSet.all();
        return (!items.isEmpty()) ? items.get(0) : null;
    }

    private void updateStock(Integer wId, Integer iId, double adjQuantity,
                             double ytd, int orderCount, int remoteCount) {
        session.execute(updateStockStmt.bind(
                adjQuantity, ytd, orderCount, remoteCount, wId, iId));
    }

    private Row selectStock(Integer wId, Integer iId) {
        ResultSet resultSet = session.execute(selectStockStmt.bind(wId, iId));
        List<Row> stocks = resultSet.all();
        return (!stocks.isEmpty()) ? stocks.get(0) : null;
    }


    private void updateCustomerOrder(int nextOId, Date curDate, int wId, int dId, int cId) {
        session.execute(updateCustomerOrderStmt.bind(
                nextOId, curDate, null, wId, dId, cId));
    }

    private void createNewOrder(int id, int dId, int wId, int cId, Date entryDate, int olCount, int allLocal,
                                String cFirst, String cMiddle, String cLast) {
        session.execute(insertOrderByTimestampStmt.bind(
                wId, dId, entryDate, id, cId, null, olCount, allLocal,
                cFirst, cMiddle, cLast));
        session.execute(insertOrderByIdStmt.bind(
                wId, dId, id, cId, entryDate, null, olCount, allLocal,
                cFirst, cMiddle, cLast));
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
