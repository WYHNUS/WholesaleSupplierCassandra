import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Set up the keyspace and schema for cassandra project.
 */
class Setup {
    static final String CONTACT_POINT = "192.168.1.101";
    static final String KEY_SPACE = "wholesale_supplier";

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

        createKeySpace();
        createSchema();
    }

    private void createKeySpace() {
        String createKeySpaceCmd = "CREATE KEYSPACE IF NOT EXISTS " + KEY_SPACE
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
                + " ) WITH CLUSTERING ORDER BY (D_ID ASC);";
        String createCustomersCmd = "CREATE TABLE " + KEY_SPACE + ".customers ("
                + " C_W_ID int, "
                + " C_D_ID int, "
                + " C_ID int, "
                + " C_FIRST text, "
                + " C_MIDDLE text, "
                + " C_STREET_1 text, "
                + " C_STREET_2 text, "
                + " C_CITY text, "
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
                + " C_LASR_ORDER int, "
                + " PRIMARY KEY (C_W_ID, C_D_ID, C_ID) "
                + " ) WITH CLUSTERING ORDER BY (C_D_ID ASC);";
        String createTopBalanceCustomersCmd = "CREATE TABLE " + KEY_SPACE + ".top_balance_customers ("
                + " W_ID int, "
                + " C_BALANCE int, "
                + " D_ID int, "
                + " C_ID decimal, "
                + " W_NAME text, "
                + " D_NAME text, "
                + " C_FIRST text, "
                + " C_MIDDLE text, "
                + " C_LAST text, "
                + " PRIMARY KEY (W_ID, C_BALANCE, D_ID, C_ID) "
                + " ) WITH CLUSTERING ORDER BY (C_BALANCE DESC);";
        String createOrdersByTimestampCmd = "CREATE TABLE " + KEY_SPACE + ".orders_by_timestamp ("
                + " O_W_ID int, "
                + " O_D_ID int, "
                + " O_ENTRY_D timestamp, "
                + " O_ID int, "
                + " O_C_ID int, "
                + " O_CARRIER_ID int, "
                + " O_OL_CNT decimal, "
                + " O_ALL_LOCAL decimal, "
                + " PRIMARY KEY (O_W_ID, O_ENTRY_D, O_D_ID, O_ID, O_C_ID) "
                + " ) WITH CLUSTERING ORDER BY (O_ENTRY_D DESC);";
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
                + " OL_DELIVERY_D timestamp, "
                + " OL_AMOUNT decimal, "
                + " OL_SUPPLY_W_ID int, "
                + " OL_QUANTITY decimal, "
                + " OL_DIST_INFO text, "
                + " PRIMARY KEY (OL_W_ID, OL_NUMBER, OL_D_ID, OL_O_ID) "
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
                + " ) WITH CLUSTERING ORDER BY (S_I_ID ASC);";

        session.execute(createWarehousesCmd);
        System.out.println("Successfully created table : warehouses");
        session.execute(createDistrictsCmd);
        System.out.println("Successfully created table : districts");
        session.execute(createCustomersCmd);
        System.out.println("Successfully created table : customers");
        session.execute(createTopBalanceCustomersCmd);
        System.out.println("Successfully created table : top_balance_customers");
        session.execute(createOrdersByTimestampCmd);
        System.out.println("Successfully created table : orders_by_timestamp");
        session.execute(createItemsCmd);
        System.out.println("Successfully created table : items");
        session.execute(createOrderLinesCmd);
        System.out.println("Successfully created table : order_lines");
        session.execute(createStocksCmd);
        System.out.println("Successfully created table : stocks");

        System.out.println("All tables are created successfully.");
    }
}
