import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Main driver class that simulates a client issuing transactions to the database.
 */
public class Main {
    /**
     * Program entry point which repeatedly reads user inputs, where each input specifies
     * an instance of one of the seven transaction types implemented in {@link Transactions}.
     * For each transaction read, the driver invokes the appropriate transaction function to process that transaction.
     * The program terminates with EOF.
     * At the end of processing all the transactions, the driver outputs to sederr the following information:
     * - Total number of transactions processed
     * - Total elapsed time for processing the transactions (in seconds)
     * - Transaction throughput (number of transactions processed per second)
     */
    public static void main(String[] args) {
        Transactions transaction = new Transactions();
        Scanner sc = new Scanner(System.in);

        while (sc.hasNext()) {
            String[] instruction = sc.nextLine().split(",");
            switch (instruction[0]) {
                case "N":
                    // new order transaction
                    int itemCount = Integer.parseInt(instruction[instruction.length - 1]);

                    List<List<Integer>> itemOrders = new ArrayList<>();
                    for (int i = 0; i < itemCount; i++) {
                        String[] orderString = sc.nextLine().split(",");
                        List<Integer> order = new ArrayList<>();
                        for (String s : orderString) {
                            order.add(Integer.parseInt(s));
                        }
                        itemOrders.add(order);
                    }

                    transaction.processOrder(Integer.parseInt(instruction[1]),
                            Integer.parseInt(instruction[2]),
                            Integer.parseInt(instruction[3]),
                            itemOrders);
                    break;
                case "P":
                    // payment transaction
                    transaction.processPayment(Integer.parseInt(instruction[1]),
                            Integer.parseInt(instruction[2]),
                            Integer.parseInt(instruction[3]),
                            Float.parseFloat(instruction[4]));
                    break;
                case "D":
                    // delivery transaction
                    transaction.processDelivery(Integer.parseInt(instruction[1]),
                            Integer.parseInt(instruction[2]));
                    break;
                case "O":
                    // Order-Status transaction
		    transaction.processOrderStatus(Integer.parseInt(instruction[1]),
                            Integer.parseInt(instruction[2]),
                            Integer.parseInt(instruction[3]));
                    break;
                case "S":
                    // Stock-Level transaction
		    transaction.processStockLevel(Integer.parseInt(instruction[1]),
                            Integer.parseInt(instruction[2]),
                            Integer.parseInt(instruction[3]),
                            Integer.parseInt(instruction[4]));
                    break;
                case "I":
                    // Popular-Item transaction
                    break;
                case "T":
                    // Top-Balance transaction
                    break;
                default:
                    System.out.println("Unsupported transaction type!");
            }
        }
    }
}
