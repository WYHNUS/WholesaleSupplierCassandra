import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Set up the keyspace and schema for cassandra project.
 */
class Setup {
    static final String CONTACT_POINT = "127.0.0.1";
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

        dropOldKeySpace();
        createKeySpace();
        createSchema();
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
                + " ) WITH CLUSTERING ORDER BY (D_ID ASC);";
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

    private void loadData() {
        loadWarehouse();
        loadDistricts();
        loadCustomers();
        // load into top_balance_customers

        // load into orders_by_timestamp

        // load into items

        // load into order_lines

        // load into stocks
    }

    private void loadCustomers() {
        String insertDistrictsCmd = "INSERT INTO " + KEY_SPACE + ".customers ("
                + " C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,"
                + " C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,"
                + " C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT,"
                + " C_DATA, C_LASR_ORDER) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                + " ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        int index = 0;
        try {
            System.out.println("Start loading data for table : customers");
            FileReader fr = new FileReader("data/customer.csv");
            BufferedReader bf = new BufferedReader(fr);

            String line;
            while ((line= bf.readLine())!=null) {
                index++;
                String[] lineData =line.split(",");
                PreparedStatement prepared = session.prepare(insertDistrictsCmd);
                BoundStatement bound = prepared.bind(
                        Integer.parseInt(lineData[0]), Integer.parseInt(lineData[1]), Integer.parseInt(lineData[2]),
                        lineData[3], lineData[4], lineData[5], lineData[6],
                        lineData[7], lineData[8], lineData[9], lineData[10], lineData[11],
                        df.parse(lineData[12]), lineData[13],
                        new BigDecimal(lineData[14]), new BigDecimal(lineData[15]), new BigDecimal(lineData[16]),
                        Float.parseFloat(lineData[17]), Integer.parseInt(lineData[18]), Integer.parseInt(lineData[19]),
                        lineData[20]);
                // todo(wangyanhao): attribute 'C_LASR_ORDER' is currently left empty, will add this in later on
                session.execute(bound);
            }

            System.out.println("Successfully loaded all data for table : customers ");
        } catch (IOException | ParseException e) {
            System.out.println("Load data failed with error : " + e.getMessage() + " at line " + index);
        }
    }

    private void loadDistricts() {
        String insertDistrictsCmd = "INSERT INTO " + KEY_SPACE + ".districts ("
                + " D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, "
                + " D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";

        try {
            System.out.println("Start loading data for table : districts");
            FileReader fr = new FileReader("data/district.csv");
            BufferedReader bf = new BufferedReader(fr);

            String line;
            while ((line= bf.readLine())!=null) {
                String[] lineData =line.split(",");
                PreparedStatement prepared = session.prepare(insertDistrictsCmd);
                BoundStatement bound = prepared.bind(
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

        try {
            System.out.println("Start loading data for table : warehouses");
            FileReader fr = new FileReader("data/warehouse.csv");
            BufferedReader bf = new BufferedReader(fr);

            String line;
            while ((line= bf.readLine())!=null) {
                String[] lineData =line.split(",");
                PreparedStatement prepared = session.prepare(insertWarehousesCmd);
                BoundStatement bound = prepared.bind(
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
