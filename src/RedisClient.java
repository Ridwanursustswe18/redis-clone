import java.util.List;
import java.util.Scanner;

public class RedisClient {
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final int NUM_CLIENTS = 50;

    public static void main(String[] args) {
        System.out.println("Multi-Threaded Redis Client");
        System.out.println("===========================");
        System.out.println("1. Run benchmark test (50 concurrent clients)");
        System.out.println("2. Run interactive mode (single client)");
        System.out.print("Select mode: ");

        Scanner scanner = new Scanner(System.in);
        int mode = scanner.nextInt();
        scanner.nextLine();

        if (mode == 1) {
            runBenchmarkMode(scanner);
        } else {
            runInteractiveMode(scanner);
        }
    }

    private static void runBenchmarkMode(Scanner scanner) {
        System.out.print("Enter Redis command to benchmark (e.g., 'SET key value' or 'GET key'): ");
        String command = scanner.nextLine().trim();
        System.out.print("Enter number of requests per client (e.g., 100): ");
        int requestsPerClient = scanner.nextInt();
        scanner.nextLine();

        List<String> commandArgs = CommandParser.parseCommandLine(command);
        if (commandArgs.isEmpty()) {
            System.out.println("Please enter a valid command");
            return;
        }

        BenchmarkConfig config = new BenchmarkConfig(
                REDIS_HOST,
                REDIS_PORT,
                NUM_CLIENTS,
                requestsPerClient,
                commandArgs
        );

        BenchmarkRunner runner = new BenchmarkRunner();
        BenchmarkResult result = runner.runBenchmark(config);
        result.print();
    }

    private static void runInteractiveMode(Scanner scanner) {
        InteractiveSession session = new InteractiveSession(REDIS_HOST, REDIS_PORT);
        session.run(scanner);
    }
}