package main.java;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.math.BigDecimal;
import java.util.Date;

public class OrderStatusTransaction {
    static final String KEY_SPACE = Setup.KEY_SPACE;

    private Session session;

    OrderStatusTransaction(Session session){
        this.session = session;
    }

    void processOrderStatus(int c_W_ID, int c_D_ID, int c_ID) {
        //get customer's name and
        String getCustomerName = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_LAST_ORDER, C_ENTRY_D, C_CARRIER_ID FROM " + KEY_SPACE + ".customers " +
                "WHERE C_W_ID = " + c_W_ID + " AND C_D_ID = " + c_D_ID + " AND C_ID = " + c_ID;
        Row row1 = session.execute(getCustomerName).one();

        String firstName = row1.getString("C_FIRST");
        String middleName = row1.getString("C_MIDDLE");
        String lastName = row1.getString("C_LAST");
        BigDecimal balance = row1.getDecimal("C_BALANCE");
        int last_order = row1.getInt("C_LAST_ORDER");
        Date entry_d = row1.getTimestamp("C_ENTRY_D");
        int carrier_id = row1.getInt("C_CARRIER_ID");

        //print out customer name and last order info
        System.out.println("Customer's first name is: " + firstName + ", middle name is: " + middleName + ", last name is: " + lastName + ".");
        System.out.println("Customer's balance is: " + balance + ".");
        System.out.println("Last order number is: " + last_order + ".");
        System.out.println("Last order entry date and time is: " + entry_d +".");
        System.out.println("Last order carrier identifier is: " + carrier_id + ".");
        System.out.println("=======Item Info is below.======");

        //for each item in the last order
        String getCustomerLastOLNumber =  "SELECT OL_NUMBER FROM " + KEY_SPACE + ".order_lines " +
                "WHERE OL_W_ID = " + c_W_ID + " AND OL_D_ID = " + c_D_ID + " AND OL_O_ID = " + last_order;

        ResultSet row2 = session.execute(getCustomerLastOLNumber);
/*
        List<Integer> itemList = new ArrayList<>();
        List<Integer> supplierWList = new ArrayList<>();
        List<Integer> quantityList = new ArrayList<>();
        List<Float> amountList = new ArrayList<>();
        List<String> deliveryDateList = new ArrayList<>();*/

        int itemID;
        int supplierWarehouse;
        BigDecimal quantity;
        BigDecimal amount;
        Date deliveryData;

        for (Row row : row2) {
            int OL_NUMBER = row.getInt("OL_NUMBER");
            String getItemInfo = "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM " + KEY_SPACE + ".order_lines " +
                    "WHERE OL_W_ID = " + c_W_ID + " AND OL_D_ID = " + c_D_ID + " AND OL_O_ID = " + last_order + " AND OL_NUMBER = " + OL_NUMBER;

            Row itemInfo = session.execute(getItemInfo).one();

            /*
            itemID = itemList.add(itemInfo.getInt("OL_I_ID"));
            supplierWList.add(itemInfo.getInt("OL_SUPPLY_W_ID"));
            quantityList.add(itemInfo.getInt("OL_QUANTITY"));
            amountList.add(itemInfo.getFloat("OL_AMOUNT"));
            deliveryDateList.add(itemInfo.getString("OL_DELIVERY_D"));*/

            itemID = itemInfo.getInt("OL_I_ID");
            supplierWarehouse = itemInfo.getInt("OL_SUPPLY_W_ID");
            quantity = itemInfo.getDecimal("OL_QUANTITY");
            amount = itemInfo.getDecimal("OL_AMOUNT");
            deliveryData = itemInfo.getTimestamp("OL_DELIVERY_D");

            //for each item in the last order, print out the info
            System.out.println("Item number: " + itemID );
            System.out.println("Supplying warehouse number: " + supplierWarehouse);
            System.out.println("Quantity ordered: " + quantity);
            System.out.println("Total price for ordered item: " + amount);
            System.out.println("Data and time of delivery: " + deliveryData);
            System.out.println("\n\n");
        }
    }
}
