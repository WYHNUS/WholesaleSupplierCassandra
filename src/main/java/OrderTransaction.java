import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.math.BigDecimal;
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
    private PreparedStatement insertOrderLineStmt;

    private static final String SELECT_WAREHOUSE =
            " SELECT W_TAX "
                    + " FROM warehouses "
                    + " WHERE W_ID = ?; ";
    private static final String SELECT_DISTRICT =
            " SELECT D_TAX, D_NEXT_O_ID "
                    + " FROM districts "
                    + " WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String SELECT_CUSTOMER =
            " SELECT C_FIRST, C_MIDDLE, C_LAST, C_CREDIT, C_DISCOUNT "
                    + " FROM customers "
                    + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?; ";
    private static final String SELECT_STOCK =
            " SELECT * "
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
                    + " SET C_LAST_ORDER = ?, C_ENTRY_D = ?, C_CARRIER_ID = ? "
                    + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?; ";
    private static final String UPDATE_STOCK =
            " UPDATE stocks "
                    + " SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? "
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
    private static final String INSERT_ORDER_LINE =
            "INSERT INTO order_lines ("
                    + " OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_I_NAME, "
                    + " OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO ) "
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";

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
        this.insertOrderLineStmt = session.prepare(INSERT_ORDER_LINE);
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
        Row district = getDistrict(wId, dId);
        Row warehouse = getWarehouse(wId);

        double dTax = district.getDecimal("D_TAX").doubleValue();
        double wTax = warehouse.getDecimal("W_TAX").doubleValue();

        int nextOId = district.getInt("D_NEXT_O_ID");
        updateDistrictNextOId(nextOId + 1, wId, dId);

        // let O_ENTRY_D = current date and time
        Date curDate = new Date();
        BigDecimal olCount = new BigDecimal(itemOrders.size());
        BigDecimal allLocal = new BigDecimal(1);
        for (List<Integer> order : itemOrders) {
            if (order.get(1) != wId) {
                allLocal = new BigDecimal(0);
            }
        }

        createNewOrder(nextOId, dId, wId, cId, curDate, olCount, allLocal,
                customer.getString("C_FIRST"), customer.getString("C_MIDDLE"), customer.getString("C_LAST"));
        updateCustomerOrder(nextOId, curDate, wId, dId, cId);

        double totalAmount= 0;
        for (int i = 0; i < itemOrders.size(); i++) {
            int iId = itemOrders.get(i).get(0);
            int iWId = itemOrders.get(i).get(1);
            int quantity = itemOrders.get(i).get(2);

            Row stock = selectStock(iWId, iId);
            double adjQuantity = stock.getDecimal("S_QUANTITY").doubleValue() - quantity;
            while (adjQuantity < 10) {
                adjQuantity += 100;
            }
            BigDecimal adjQuantityDecimal = new BigDecimal(adjQuantity);

            updateStock(iWId, iId, adjQuantityDecimal,
                    stock.getDecimal("S_YTD").add(new BigDecimal(quantity)),
                    stock.getInt("S_ORDER_CNT") + 1,
                    (iWId != wId)
                            ? stock.getInt("S_REMOTE_CNT") + 1
                            : stock.getInt("S_REMOTE_CNT"));

            Row item = selectItem(iId);
            String itemName = item.getString("I_NAME");
            BigDecimal itemAmount = item.getDecimal("I_PRICE").multiply(new BigDecimal(quantity));
            totalAmount += itemAmount.doubleValue();
            createNewOrderLine(wId, dId, nextOId, i, iId, itemName,
                    itemAmount, iWId, new BigDecimal(quantity), stock.getString(getDistrictStringId(dId)));

            // log
            System.out.printf(
                    "itemId: %d, itemName: %s, warehouseId: %d, quantity: %d, OL_AMOUNT: %f, S_QUANTITY: %f \n",
                    iId, itemName, iWId, quantity, itemAmount, adjQuantity);
        }

        totalAmount = totalAmount * (1 + dTax + wTax) * (1 - customer.getDecimal("C_DISCOUNT").doubleValue());

        // log
        System.out.printf(
                "customer with C_W_ID: %d, C_D_ID: %d, C_ID: %d, C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %s \n",
                wId, dId, cId, customer.getString("C_LAST"),
                customer.getString("C_CREDIT"), customer.getDecimal("C_DISCOUNT"));
        System.out.printf("Warehouse tax rate: %f, District tax rate: %f \n", wTax, dTax);
        System.out.printf("Order number: %d, entry date: %s \n", nextOId, curDate);
        System.out.printf("Number of items: %d, total amount for order: %f ", olCount.intValue(), totalAmount);
    }

    private void createNewOrderLine(int wId, int dId, int oId, int olNumber, int iId, String iName,
                                    BigDecimal itemAmount, int supplyWId, BigDecimal quantity, String distInfo) {
        session.execute(insertOrderLineStmt.bind(
                wId, dId, oId, olNumber, iId, iName, itemAmount, supplyWId, quantity, distInfo));
    }

    private String getDistrictStringId(int dId) {
        if (dId < 10) {
            return "S_DIST_0" + dId;
        } else {
            return "S_DIST_10";
        }
    }

    private Row selectItem(int iId) {
        ResultSet resultSet = session.execute(selectItemStmt.bind(iId));
        List<Row> items = resultSet.all();
        return (!items.isEmpty()) ? items.get(0) : null;
    }

    private void updateStock(Integer wId, Integer iId, BigDecimal adjQuantity,
                             BigDecimal ytd, int orderCount, int remoteCount) {
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

    private void createNewOrder(int id, int dId, int wId, int cId, Date entryDate, BigDecimal olCount, BigDecimal allLocal,
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

    private Row getWarehouse(int wId) {
        ResultSet resultSet = session.execute(selectWarehouseStmt.bind(wId));
        List<Row> warehouses = resultSet.all();
        return (!warehouses.isEmpty()) ? warehouses.get(0) : null;
    }

    private Row getDistrict(int wId, int dId) {
        ResultSet resultSet = session.execute(selectDistrictStmt.bind(wId, dId));
        List<Row> districts = resultSet.all();
        return (!districts.isEmpty()) ? districts.get(0) : null;
    }
}
