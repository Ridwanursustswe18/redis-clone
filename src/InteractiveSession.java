import java.util.List;
import java.util.Scanner;

class InteractiveSession {
    private final String host;
    private final int port;

    public InteractiveSession(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void run(Scanner scanner) {
        try (RedisConnection connection = new RedisConnection(host, port)) {
            System.out.println("Connected to Redis server on " + host + ":" + port);
            System.out.println("Enter Redis commands (type 'exit' to quit):");

            while (true) {
                System.out.print("> ");
                String commandLine = scanner.nextLine().trim();

                if (commandLine.equalsIgnoreCase("exit")) {
                    break;
                }

                List<String> commandArgs = CommandParser.parseCommandLine(commandLine);

                if (commandArgs.isEmpty()) {
                    System.out.println("Please enter a valid command");
                    continue;
                }

                try {
                    long startTime = System.nanoTime();
                    Object response = connection.executeCommand(commandArgs);
                    long endTime = System.nanoTime();

                    double latencyMs = (endTime - startTime) / 1_000_000.0;

                    ResponseFormatter.printResponse(response);
                    System.out.println("Command latency: " + String.format("%.3f", latencyMs) + " msec");
                } catch (Exception e) {
                    System.out.println("Error executing command: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Connection error: " + e.getMessage());
        }
    }
}
