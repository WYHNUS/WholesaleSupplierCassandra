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
                    break;
                case "P":
                    // payment transaction
                    break;
                case "D":
                    // delivery transaction
                    break;
                case "O":
                    // Order-Status transaction
                    break;
                case "S":
                    // Stock-Level transaction
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
