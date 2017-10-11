import com.datastax.driver.core.Session;

import java.util.List;

class OrderTransaction {
    private Session session;

    OrderTransaction(Session session) {
        this.session = session;
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
        // query customers using (wId, dID, cId)
        // query districts using (wId, dID) to get D_TAX
        // query warehouses using (wId) to get W_TAX

        // read N=D_NEXT_O_ID from districts using (wId, dID)

        // update district (wId, dID) by increase D_NEXT_O_ID by one

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
}
