package main.java;

import com.datastax.driver.core.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Set up the keyspace and schema for cassandra project.
 * Bulk load data from .csv files.
 */
class Setup {
    static final String CONTACT_POINT;
    static final String KEY_SPACE;

    private static final String CONTACT_POINT_KEY = "CONTACT_POINT";
    private static final String KEY_SPACE_KEY = "KEY_SPACE";

    static {
        Map<String, String> configMap = new HashMap<>();
        String line;
        try {
            FileReader fr = new FileReader(".env");
            BufferedReader bf = new BufferedReader(fr);

            while ((line = bf.readLine()) != null) {
                String[] lineData =line.split("=");
                switch (lineData[0]) {
                    case CONTACT_POINT_KEY:
                        configMap.put(CONTACT_POINT_KEY, lineData[1].trim());
                        break;
                    case KEY_SPACE_KEY:
                        configMap.put(KEY_SPACE_KEY, lineData[1].trim());
                        break;
                    default:
                        // do nothing
                }
            }
        } catch (IOException e) {
            System.out.println("An error occurred while read in env file.");
        }
        CONTACT_POINT = configMap.get(CONTACT_POINT_KEY);
        KEY_SPACE = configMap.get(KEY_SPACE_KEY);
    }

    private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private Session session;

    public static void main(String[] args) {
        Setup s = new Setup();
        s.run();
    }

    private void run() {
        Cluster cluster = Cluster.builder()
                .addContactPoint(CONTACT_POINT)
                .build();
        session = cluster.connect();

        dropOldKeySpace();
        createKeySpace();
        createSchema();
        createView();
        loadData();
    }

    private void dropOldKeySpace() {
        String dropKeySpaceCmd = "DROP KEYSPACE IF EXISTS " + KEY_SPACE;
        session.execute(dropKeySpaceCmd);
    }

    private void createKeySpace() {
        String createKeySpaceCmd = "CREATE KEYSPACE " + KEY_SPACE
                + " WITH replication = {"
                + " 'class' : " + "'SimpleStrategy'" + ","
                + " 'replication_factor' : " + "1" + "};";
        session.execute(createKeySpaceCmd);
        System.out.println("Successfully created keyspace : " + KEY_SPACE);
    }

    private void createSchema() {
        String createWarehousesCmd = "CREATE TABLE " + KEY_SPACE + ".warehouses ("
                + " W_ID int, "
                + " W_NAME text, "
                + " W_STREET_1 text, "
                + " W_STREET_2 text, "
                + " W_CITY text, "
                + " W_STATE text, "
                + " W_ZIP text, "
                + " W_TAX decimal, "
                + " W_YTD decimal, "
                + " PRIMARY KEY (W_ID) "
                + " );";
        String createDistrictsCmd = "CREATE TABLE " + KEY_SPACE + ".districts ("
                + " D_W_ID int, "
                + " D_ID int, "
                + " D_NAME text, "
                + " D_STREET_1 text, "
                + " D_STREET_2 text, "
                + " D_CITY text, "
                + " D_STATE text, "
                + " D_ZIP text, "
                + " D_TAX decimal, "
                + " D_YTD decimal, "
                + " D_NEXT_O_ID int, "
                + " PRIMARY KEY (D_W_ID, D_ID) "
                + " );";
        String createCustomersCmd = "CREATE TABLE " + KEY_SPACE + ".customers ("
                + " C_W_ID int, "
                + " C_D_ID int, "
                + " C_ID int, "
                + " C_FIRST text, "
                + " C_MIDDLE text, "
                + " C_LAST text, "
                + " C_STREET_1 text, "
                + " C_STREET_2 text, "
                + " C_CITY text, "
                + " C_STATE text, "
                + " C_ZIP text, "
                + " C_PHONE text, "
                + " C_SINCE timestamp, "
                + " C_CREDIT text, "
                + " C_CREDIT_LIM decimal, "
                + " C_DISCOUNT decimal, "
                + " C_BALANCE decimal, "
                + " C_YTD_PAYMENT float, "
                + " C_PAYMENT_CNT int, "
                + " C_DELIVERY_CNT int, "
                + " C_DATA text, "
                + " C_LAST_ORDER int, "
                + " C_ENTRY_D timestamp, "
                + " C_CARRIER_ID int, "
                + " PRIMARY KEY (C_W_ID, C_D_ID, C_ID) "
                + " );";
//        String createTopBalanceCustomersCmd = "CREATE TABLE " + KEY_SPACE + ".top_balance_customers ("
//                + " W_ID int, "
//                + " C_BALANCE int, "
//                + " D_ID int, "
//                + " C_ID decimal, "
//                + " W_NAME text, "
//                + " D_NAME text, "
//                + " C_FIRST text, "
//                + " C_MIDDLE text, "
//                + " C_LAST text, "
//                + " PRIMARY KEY (W_ID, C_BALANCE, D_ID, C_ID) "
//                + " ) WITH CLUSTERING ORDER BY (C_BALANCE DESC);";
        String createOrdersByTimestampCmd = "CREATE TABLE " + KEY_SPACE + ".orders_by_timestamp ("
                + " O_W_ID int, "
                + " O_D_ID int, "
                + " O_ENTRY_D timestamp, "
                + " O_ID int, "
                + " O_C_ID int, "
                + " O_CARRIER_ID int, "
                + " O_OL_CNT decimal, "
                + " O_ALL_LOCAL decimal, "
                + " O_C_FIRST text, "
                + " O_C_MIDDLE text, "
                + " O_C_LAST text, "
                + " PRIMARY KEY ((O_W_ID, O_D_ID, O_ID, O_C_ID), O_ENTRY_D) "
                + " ) WITH CLUSTERING ORDER BY (O_ENTRY_D DESC);";
        String createOrdersByIdCmd = "CREATE TABLE " + KEY_SPACE + ".orders_by_id ("
                + " O_W_ID int, "
                + " O_D_ID int, "
                + " O_ID int, "
                + " O_C_ID int, "
                + " O_ENTRY_D timestamp, "
                + " O_CARRIER_ID int, "
                + " O_OL_CNT decimal, "
                + " O_ALL_LOCAL decimal, "
                + " O_C_FIRST text, "
                + " O_C_MIDDLE text, "
                + " O_C_LAST text, "
                + " PRIMARY KEY ((O_W_ID, O_D_ID), O_ID, O_C_ID) "
                + " ) WITH CLUSTERING ORDER BY (O_ID ASC);";
        String createItemsCmd = "CREATE TABLE " + KEY_SPACE + ".items ("
                + " I_ID int, "
                + " I_NAME text, "
                + " I_PRICE decimal, "
                + " I_IM_ID int, "
                + " I_DATA text, "
                + " PRIMARY KEY (I_ID) "
                + " );";
        String createOrderLinesCmd = "CREATE TABLE " + KEY_SPACE + ".order_lines ("
                + " OL_W_ID int, "
                + " OL_D_ID int, "
                + " OL_O_ID int, "
                + " OL_NUMBER int, "
                + " OL_I_ID int, "
                + " OL_I_NAME text, "
                + " OL_DELIVERY_D timestamp, "
                + " OL_AMOUNT decimal, "
                + " OL_SUPPLY_W_ID int, "
                + " OL_QUANTITY decimal, "
                + " OL_DIST_INFO text, "
                + " PRIMARY KEY ((OL_W_ID, OL_D_ID, OL_O_ID), OL_NUMBER) "
                + " ) WITH CLUSTERING ORDER BY (OL_NUMBER ASC);";
        String createStocksCmd = "CREATE TABLE " + KEY_SPACE + ".stocks ("
                + " S_W_ID int, "
                + " S_I_ID int, "
                + " S_QUANTITY decimal, "
                + " S_YTD decimal, "
                + " S_ORDER_CNT int, "
                + " S_REMOTE_CNT int, "
                + " S_DIST_01 text, "
                + " S_DIST_02 text, "
                + " S_DIST_03 text, "
                + " S_DIST_04 text, "
                + " S_DIST_05 text, "
                + " S_DIST_06 text, "
                + " S_DIST_07 text, "
                + " S_DIST_08 text, "
                + " S_DIST_09 text, "
                + " S_DIST_10 text, "
                + " S_DATA text, "
                + " PRIMARY KEY (S_W_ID, S_I_ID) "
                + " );";

        session.execute(createWarehousesCmd);
        System.out.println("Successfully created table : warehouses");
        session.execute(createDistrictsCmd);
        System.out.println("Successfully created table : districts");
        session.execute(createCustomersCmd);
        System.out.println("Successfully created table : customers");
        // this table is temporarily removed since its update would be inefficient for Payment Transaction
        // will enable it again or remove it permanently when a better plan comes up
//        session.execute(createTopBalanceCustomersCmd);
//        System.out.println("Successfully created table : top_balance_customers");
        session.execute(createOrdersByTimestampCmd);
        System.out.println("Successfully created table : orders_by_timestamp");
        session.execute(createOrdersByIdCmd);
        System.out.println("Successfully created table : orders_by_id");
        session.execute(createItemsCmd);
        System.out.println("Successfully created table : items");
        session.execute(createOrderLinesCmd);
        System.out.println("Successfully created table : order_lines");
        session.execute(createStocksCmd);
        System.out.println("Successfully created table : stocks");

        System.out.println("All tables are created successfully.");
    }

    private void loadData() {
        loadWarehouse();
        loadDistricts();
        loadCustomerAndOrder();
        loadItemsAndOrderLines();
        loadStock();
        System.out.println("All data are loaded successfully.");
    }

    private void createView() {
        String createViewCmd = "CREATE MATERIALIZED VIEW " + KEY_SPACE + ".customers_balances AS "
                + " SELECT C_ID from " + KEY_SPACE + ".customers "
                + " WHERE C_W_ID IS NOT NULL AND C_D_ID IS NOT NULL AND C_ID IS NOT NULL "
                + " AND C_BALANCE IS NOT NULL "
                + " PRIMARY KEY (C_BALANCE, C_W_ID, C_D_ID, C_ID)"
                + " WITH CLUSTERING ORDER BY (C_BALANCE DESC)";
        session.execute(createViewCmd);
        System.out.println("Successfully created materialized view : customers_balances");
    }

    private void loadStock() {
        String insertStocksCmd = "INSERT INTO " + KEY_SPACE + ".stocks ("
                + " S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, "
                + " S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, "
                + " S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertStockStmt = session.prepare(insertStocksCmd);
        String line;

        try {
            System.out.println("Start loading data for table : stocks");
            FileReader fr = new FileReader("data/stock.csv");
            BufferedReader bf = new BufferedReader(fr);

            while ((line = bf.readLine()) != null) {
                String[] lineData =line.split(",");
                BoundStatement bound = insertStockStmt.bind(
                        Integer.parseInt(lineData[0]), Integer.parseInt(lineData[1]),
                        new BigDecimal(lineData[2]), new BigDecimal(lineData[3]),
                        Integer.parseInt(lineData[4]), Integer.parseInt(lineData[5]),
                        lineData[6], lineData[7], lineData[8], lineData[9], lineData[10],
                        lineData[11], lineData[12], lineData[13], lineData[14], lineData[15],
                        lineData[16]);
                session.execute(bound);
            }

            System.out.println("Successfully loaded all data for table : stocks ");
        } catch (IOException e) {
            System.out.println("Load data failed with error : " + e.getMessage());
        }
    }

    private void loadItemsAndOrderLines() {
        String insertOrderLinesCmd = "INSERT INTO " + KEY_SPACE + ".order_lines ("
                + " OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_I_NAME, "
                + " OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        String insertItemsCmd = "INSERT INTO " + KEY_SPACE + ".items ("
                + " I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA ) "
                + " VALUES (?, ?, ?, ?, ?); ";
        PreparedStatement insertOrderLinesStmt = session.prepare(insertOrderLinesCmd);
        PreparedStatement insertItemStmt = session.prepare(insertItemsCmd);

        FileReader fr;
        BufferedReader bf;
        String line;
        Map<Integer, String> hm = new HashMap<>();

        try {
            System.out.println("Start loading data for table : items");
            fr = new FileReader("data/item.csv");
            bf = new BufferedReader(fr);

            while ((line = bf.readLine()) != null) {
                String[] lineData =line.split(",");
                int itemId = Integer.parseInt(lineData[0]);
                String itemName = lineData[1];
                hm.put(itemId, itemName);
                BoundStatement bound = insertItemStmt.bind(
                        itemId, itemName, new BigDecimal(lineData[2]),
                        Integer.parseInt(lineData[3]), lineData[4]);
                session.execute(bound);
            }
            System.out.println("Successfully loaded all data for table : items ");

            System.out.println("Start loading data for table : order_lines");
            fr = new FileReader("data/order-line.csv");
            bf = new BufferedReader(fr);

            while ((line = bf.readLine()) != null) {
                String[] lineData =line.split(",");
                Date date;
                if (lineData[5].equals("null")) {
                    date = null;
                } else {
                    date = DF.parse(lineData[5]);
                }
                int itemId = Integer.parseInt(lineData[4]);
                BoundStatement bound = insertOrderLinesStmt.bind(
                        Integer.parseInt(lineData[0]), Integer.parseInt(lineData[1]), Integer.parseInt(lineData[2]),
                        Integer.parseInt(lineData[3]), itemId, hm.get(itemId),
                        date, new BigDecimal(lineData[6]),
                        Integer.parseInt(lineData[7]), new BigDecimal(lineData[8]), lineData[9]);
                if (date == null) {
                    bound.unset(5 /* don't set date if null */);
                }
                session.execute(bound);
            }

            System.out.println("Successfully loaded all data for table : order_lines ");
        } catch (IOException | ParseException e) {
            System.out.println("Load data failed with error : " + e.getMessage());
        }
    }

    // load order and customer together as customer make use of data from order file
    private void loadCustomerAndOrder() {
        String insertCustomerCmd = "INSERT INTO " + KEY_SPACE + ".customers ("
                + " C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,"
                + " C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,"
                + " C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT,"
                + " C_DATA, C_LAST_ORDER, C_ENTRY_D, C_CARRIER_ID) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                + " ?, ?, ?, ?); ";
        String insertOrdersByTimestampCmd = "INSERT INTO " + KEY_SPACE + ".orders_by_timestamp ("
                + " O_W_ID, O_D_ID, O_ENTRY_D, O_ID, "
                + " O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, "
                + " O_C_FIRST, O_C_MIDDLE, O_C_LAST ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        String insertOrdersByIdCmd = "INSERT INTO " + KEY_SPACE + ".orders_by_id ("
                + " O_W_ID, O_D_ID, O_ID, O_C_ID, "
                + " O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, "
                + " O_C_FIRST, O_C_MIDDLE, O_C_LAST ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertCustomerStmt = session.prepare(insertCustomerCmd);
        PreparedStatement insertOrderByTimestampStmt = session.prepare(insertOrdersByTimestampCmd);
        PreparedStatement insertOrderByIdStmt = session.prepare(insertOrdersByIdCmd);

        FileReader fr;
        BufferedReader bf;
        String line;
        Set<Order> orderSet = new HashSet<>();
        Map<Integer, Triple<Integer, Date, Integer>> orderMap = new HashMap<>();
        Map<Integer, Triple<String, String, String>> customerMap = new HashMap<>();

        try {
            // load order data
            System.out.println("Read data from order file.");
            fr = new FileReader("data/order.csv");
            bf = new BufferedReader(fr);

            while ((line = bf.readLine()) != null) {
                String[] lineData =line.split(",");

                // set <C_LAST_ORDER, C_ENTRY_D, C_CARRIER_ID> map
                int orderId = Integer.parseInt(lineData[2]);
                int customerId = Integer.parseInt(lineData[3]);
                Date entryDate = DF.parse(lineData[7]);
                int carrierId = -1; // default value -1 to indicate null
                if (!lineData[4].equals("null")) {
                    carrierId = Integer.parseInt(lineData[4]);
                }
                if (!orderMap.containsKey(customerId)) {
                    orderMap.put(customerId, new Triple<>(orderId, entryDate, carrierId));
                } else if (orderMap.get(customerId).first < orderId) {
                    orderMap.put(customerId, new Triple<>(orderId, entryDate, carrierId));
                }

                // save locally first, later load into DB
                Order curOrder = (new Order())
                        .setWId(Integer.parseInt(lineData[0]))
                        .setDId(Integer.parseInt(lineData[1]))
                        .setEntryDate(entryDate)
                        .setId(orderId)
                        .setCId(customerId)
                        .setCarrierId(carrierId)
                        .setOlCnt(new BigDecimal(lineData[5]))
                        .setAllLocal(new BigDecimal(lineData[6]));
                orderSet.add(curOrder);
            }
            System.out.println("Successfully loaded all data from order file.");

            // load customer
            System.out.println("Start loading data for table : customers");
            fr = new FileReader("data/customer.csv");
            bf = new BufferedReader(fr);

            while ((line = bf.readLine()) != null) {
                String[] lineData =line.split(",");

                int customerId = Integer.parseInt(lineData[2]);
                String firstName = lineData[3];
                String middleName = lineData[4];
                String lastName = lineData[5];
                customerMap.put(customerId, new Triple<>(firstName, middleName, lastName));

                // retrieve <C_LAST_ORDER, C_ENTRY_D, C_CARRIER_ID> triple
                Triple triple = orderMap.get(customerId);
                BoundStatement bound = insertCustomerStmt.bind(
                        Integer.parseInt(lineData[0]), Integer.parseInt(lineData[1]), customerId,
                        firstName, middleName, lastName, lineData[6], lineData[7],
                        lineData[8], lineData[9], lineData[10], lineData[11],
                        DF.parse(lineData[12]), lineData[13],
                        new BigDecimal(lineData[14]), new BigDecimal(lineData[15]), new BigDecimal(lineData[16]),
                        Float.parseFloat(lineData[17]), Integer.parseInt(lineData[18]), Integer.parseInt(lineData[19]),
                        lineData[20], triple.first, triple.second, triple.third);
                session.execute(bound);
            }
            System.out.println("Successfully loaded all data for table : customers ");

            // load order data into DB
            System.out.println("Start loading data for table : orders_by_timestamp and orders_by_id");
            for (Order order : orderSet) {
                Triple triple = customerMap.get(order.cId);

                BoundStatement boundByTimestamp = insertOrderByTimestampStmt.bind(
                        order.wId, order.dId, order.entryDate, order.id,
                        order.cId, order.carrierId, order.olCnt, order.allLocal,
                        triple.first, triple.second, triple.third);
                BoundStatement boundById = insertOrderByIdStmt.bind(
                        order.wId, order.dId, order.id, order.cId,
                        order.entryDate, order.carrierId, order.olCnt, order.allLocal,
                        triple.first, triple.second, triple.third);
                session.execute(boundByTimestamp);
                session.execute(boundById);
            }
            System.out.println("Successfully loaded all data for table : orders_by_timestamp and orders_by_id ");
        } catch (IOException | ParseException e) {
            System.out.println("Load data failed with error : " + e.getMessage());
        }
    }

    private void loadDistricts() {
        String insertDistrictsCmd = "INSERT INTO " + KEY_SPACE + ".districts ("
                + " D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, "
                + " D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertByDistrictStmt = session.prepare(insertDistrictsCmd);
        String line;

        try {
            System.out.println("Start loading data for table : districts");
            FileReader fr = new FileReader("data/district.csv");
            BufferedReader bf = new BufferedReader(fr);

            while ((line = bf.readLine()) != null) {
                String[] lineData =line.split(",");
                BoundStatement bound = insertByDistrictStmt.bind(
                        Integer.parseInt(lineData[0]), Integer.parseInt(lineData[1]),
                        lineData[2], lineData[3], lineData[4], lineData[5], lineData[6], lineData[7],
                        new BigDecimal(lineData[8]), new BigDecimal(lineData[9]),
                        Integer.parseInt(lineData[10]));
                session.execute(bound);
            }

            System.out.println("Successfully loaded all data for table : districts ");
        } catch (IOException e) {
            System.out.println("Load data failed with error : " + e.getMessage());
        }
    }

    private void loadWarehouse() {
        String insertWarehousesCmd = "INSERT INTO " + KEY_SPACE + ".warehouses ("
                + " W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, "
                + " W_STATE, W_ZIP, W_TAX, W_YTD ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertWarehouseStmt = session.prepare(insertWarehousesCmd);
        String line;

        try {
            System.out.println("Start loading data for table : warehouses");
            FileReader fr = new FileReader("data/warehouse.csv");
            BufferedReader bf = new BufferedReader(fr);
            while ((line = bf.readLine()) != null) {
                String[] lineData =line.split(",");
                BoundStatement bound = insertWarehouseStmt.bind(
                        Integer.parseInt(lineData[0]), lineData[1], lineData[2],
                        lineData[3], lineData[4], lineData[5], lineData[6],
                        new BigDecimal(lineData[7]), new BigDecimal(lineData[8]));
                session.execute(bound);
            }

            System.out.println("Successfully loaded all data for table : warehouses ");
        } catch (IOException e) {
            System.out.println("Load data failed with error : " + e.getMessage());
        }
    }
}

class Order {
    int wId;
    int dId;
    Date entryDate;
    int id;
    int cId;
    int carrierId = -1; // indicate not valid
    BigDecimal olCnt;
    BigDecimal allLocal;
    String cFirst;
    String cMiddle;
    String cLast;

    Order() {}

    Order setWId(int wId) {
        this.wId = wId;
        return this;
    }
    Order setDId(int dId) {
        this.dId = dId;
        return this;
    }
    Order setEntryDate(Date entryDate) {
        this.entryDate = entryDate;
        return this;
    }
    Order setId(int id) {
        this.id = id;
        return this;
    }
    Order setCId(int cId) {
        this.cId = cId;
        return this;
    }
    Order setCarrierId(int carrierId) {
        this.carrierId = carrierId;
        return this;
    }
    Order setOlCnt(BigDecimal olCnt) {
        this.olCnt = olCnt;
        return this;
    }
    Order setAllLocal(BigDecimal allLocal) {
        this.allLocal = allLocal;
        return this;
    }
    Order setCFirst(String cFirst) {
        this.cFirst = cFirst;
        return this;
    }
    Order setCMiddle(String cMiddle) {
        this.cMiddle = cMiddle;
        return this;
    }
    Order setCLast(String cLast) {
        this.cLast = cLast;
        return this;
    }
}

class Triple<T, U, V> {
    final T first;
    final U second;
    final V third;

    Triple(T first, U second, V third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
}
