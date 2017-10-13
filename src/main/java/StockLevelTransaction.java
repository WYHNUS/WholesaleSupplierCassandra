import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

public class StockLevelTransaction {
    static final String CONTACT_POINT = Setup.CONTACT_POINT;
    static final String KEY_SPACE = Setup.KEY_SPACE;

    private Session session;

    StockLevelTransaction(Session session){
        this.session = session;
    }

    void processStockLevel(int w_ID, int d_ID, int T, int L) {

        //get Last L order number

        String getLastLOrderNumber = "SELECT O_ID FROM " + KEY_SPACE +".orders_by_timestamp " +
                "WHERE O_W_ID = " + w_ID + " AND O_D_ID = " + d_ID + " LIMIT " + L + " allow filtering;";
        ResultSet lastOrderList = session.execute(getLastLOrderNumber);


        Set<Integer> items = new HashSet<>();
        //HashMap<Integer, Integer> itemQuantity = new HashMap<Integer, Integer>();


        for (Row row1 : lastOrderList) {
            int lastOrder = row1.getInt("O_ID");

            //get all items
            String getAllItems = "SELECT OL_I_ID FROM " + KEY_SPACE + ".order_lines " +
                    "WHERE OL_W_ID = " + w_ID + " AND OL_D_ID = " + d_ID + " AND OL_O_ID = " + lastOrder;
            ResultSet itemList = session.execute(getAllItems);

            for (Row row2 : itemList) {
                //add the item id into the items list
                int itemID = row2.getInt("OL_I_ID");
                items.add(itemID);
            }

        }

        for (int itemID : items) {
            //check if item quantity in the stock is below the threshold
            String getQuantity = "SELECT S_QUANTITY FROM " + KEY_SPACE + ".stocks " +
                    "WHERE S_W_ID = " + w_ID + " AND S_I_ID = " + itemID + ";";

            BigDecimal quantity = session.execute(getQuantity).one().getDecimal("S_QUANTITY");

            BigDecimal decimalT = new BigDecimal(T);

            if(quantity.compareTo(decimalT) == -1) {
                //itemQuantity.put(itemID, quantity);
                System.out.println("Items " + itemID + " stock quantity at "+ w_ID + " is below the threshold; its quantity number is: " + quantity + ".");
            }
        }
        System.out.println("\n\n");
    }
}
