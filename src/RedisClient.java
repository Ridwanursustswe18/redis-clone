import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisClient {
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final int NUM_CLIENTS = 50;
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger errorCount = new AtomicInteger(0);
    private static final List<Long> responseTimes = Collections.synchronizedList(new ArrayList<>());

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

        List<String> commandArgs = parseCommandLine(command);
        if (commandArgs.isEmpty()) {
            System.out.println("Please enter a valid command");
            return;
        }

        System.out.println("\nStarting benchmark with:");
        System.out.println("- " + NUM_CLIENTS + " concurrent clients");
        System.out.println("- " + requestsPerClient + " requests per client");
        System.out.println("- Command: " + command);
        System.out.println("- Total requests: " + (NUM_CLIENTS * requestsPerClient));

        ExecutorService executor = Executors.newFixedThreadPool(NUM_CLIENTS);
        CountDownLatch latch = new CountDownLatch(NUM_CLIENTS);
        long startTime = System.currentTimeMillis();

        // Clear response times from previous runs
        responseTimes.clear();
        successCount.set(0);
        errorCount.set(0);

        for (int i = 0; i < NUM_CLIENTS; i++) {
            final int clientId = i;
            executor.submit(() -> {
                try {
                    runClientBenchmark(clientId, commandArgs, requestsPerClient);
                } catch (Exception e) {
                    System.err.println("Client " + clientId + " error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            long endTime = System.currentTimeMillis();
            double totalTimeSeconds = (endTime - startTime) / 1000.0;
            int totalRequests = NUM_CLIENTS * requestsPerClient;
            double requestsPerSecond = totalRequests / totalTimeSeconds;

            // Calculate percentiles
            double p50 = calculatePercentile(50);
            double p90 = calculatePercentile(90);
            double p99 = calculatePercentile(99);

            System.out.println("\nBenchmark Results:");
            System.out.println("=================");
            System.out.println("Total time: " + totalTimeSeconds + " seconds");
            System.out.println("Total requests: " + totalRequests);
            System.out.println("Successful requests: " + successCount.get());
            System.out.println("Failed requests: " + errorCount.get());
            System.out.println("Requests per second: " + String.format("%.2f", requestsPerSecond));
            System.out.println("p50 latency: " + String.format("%.3f", p50) + " msec");
            System.out.println("p90 latency: " + String.format("%.3f", p90) + " msec");
            System.out.println("p99 latency: " + String.format("%.3f", p99) + " msec");

        } catch (InterruptedException e) {
            System.err.println("Benchmark interrupted: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    private static void runClientBenchmark(int clientId, List<String> commandArgs, int requestCount) {
        try (Socket socket = new Socket(REDIS_HOST, REDIS_PORT);
             OutputStream output = socket.getOutputStream();
             InputStream input = socket.getInputStream()) {

            byte[] serializedCommand = serializeCommand(commandArgs);

            for (int i = 0; i < requestCount; i++) {
                try {
                    long startTime = System.nanoTime(); // Start timing in nanoseconds for higher precision

                    output.write(serializedCommand);
                    output.flush();
                    deserializeResponse(input);

                    long endTime = System.nanoTime();
                    long latencyNanos = endTime - startTime;
                    double latencyMs = latencyNanos / 1_000_000.0; // Convert to milliseconds

                    responseTimes.add((long)latencyMs); // Store latency in milliseconds
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            }
        } catch (IOException e) {
            System.err.println("Client " + clientId + " connection error: " + e.getMessage());
        }
    }

    private static void runInteractiveMode(Scanner scanner) {
        try (Socket socket = new Socket(REDIS_HOST, REDIS_PORT);
             OutputStream output = socket.getOutputStream();
             InputStream input = socket.getInputStream()) {

            System.out.println("Connected to Redis server on " + REDIS_HOST + ":" + REDIS_PORT);
            System.out.println("Enter Redis commands (type 'exit' to quit):");

            while (true) {
                System.out.print("> ");
                String commandLine = scanner.nextLine().trim();

                if (commandLine.equalsIgnoreCase("exit")) {
                    break;
                }

                List<String> commandArgs = parseCommandLine(commandLine);
                if (commandArgs.isEmpty()) {
                    System.out.println("Please enter a valid command");
                    continue;
                }

                try {
                    long startTime = System.nanoTime();

                    byte[] serializedCommand = serializeCommand(commandArgs);
                    output.write(serializedCommand);
                    output.flush();

                    Object response = deserializeResponse(input);

                    long endTime = System.nanoTime();
                    double latencyMs = (endTime - startTime) / 1_000_000.0;

                    printResponse(response);
                    System.out.println("Command latency: " + String.format("%.3f", latencyMs) + " msec");
                } catch (Exception e) {
                    System.out.println("Error executing command: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Connection error: " + e.getMessage());
        }
    }

    private static void printResponse(Object response) {
        if (response == null) {
            System.out.println("(nil)");
        } else if (response instanceof Object[]) {
            Object[] array = (Object[]) response;
            System.out.println("Array[" + array.length + "]:");
            for (int i = 0; i < array.length; i++) {
                System.out.println("  " + i + ") " + array[i]);
            }
        } else {
            System.out.println(response);
        }
    }

    public static byte[] serializeCommand(List<String> commandParts) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(commandParts.size()).append("\r\n");
        for (String part : commandParts) {
            sb.append("$").append(part.length()).append("\r\n");
            sb.append(part).append("\r\n");
        }
        return sb.toString().getBytes();
    }

    public static Object deserializeResponse(InputStream inputStream) throws IOException {
        int prefix = inputStream.read();
        if (prefix == -1) throw new IOException("Unexpected end of stream");

        return switch ((char) prefix) {
            case '+' -> readSimpleString(inputStream);
            case '-' -> {
                String error = readLine(inputStream);
                yield "Error: " + error;
            }
            case ':' -> Long.parseLong(readLine(inputStream));
            case '$' -> readBulkString(inputStream);
            case '*' -> readArray(inputStream);
            default -> throw new IOException("Unexpected RESP prefix: " + (char) prefix);
        };
    }

    private static String readSimpleString(InputStream inputStream) throws IOException {
        return readLine(inputStream);
    }

    private static String readBulkString(InputStream inputStream) throws IOException {
        int length = Integer.parseInt(readLine(inputStream));
        if (length == -1) return null; // Null bulk string

        byte[] buffer = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length) {
            int read = inputStream.read(buffer, bytesRead, length - bytesRead);
            if (read == -1) throw new IOException("Unexpected end of stream");
            bytesRead += read;
        }

        // Skip CRLF
        inputStream.read(); // CR
        inputStream.read(); // LF

        return new String(buffer);
    }

    private static Object[] readArray(InputStream inputStream) throws IOException {
        int count = Integer.parseInt(readLine(inputStream));
        if (count == -1) return null; // Null array

        Object[] array = new Object[count];
        for (int i = 0; i < count; i++) {
            int prefix = inputStream.read();
            if (prefix == -1) throw new IOException("Unexpected end of stream");

            array[i] = switch ((char) prefix) {
                case '+' -> readSimpleString(inputStream);
                case '-' -> "Error: " + readLine(inputStream);
                case ':' -> Long.parseLong(readLine(inputStream));
                case '$' -> readBulkString(inputStream);
                case '*' -> readArray(inputStream);
                default -> throw new IOException("Unexpected RESP prefix: " + (char) prefix);
            };
        }
        return array;
    }

    private static String readLine(InputStream inputStream) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = inputStream.read()) != -1) {
            if (c == '\r') {
                int next = inputStream.read();
                if (next == '\n') {
                    break;
                } else {
                    sb.append((char) c);
                    sb.append((char) next);
                }
            } else {
                sb.append((char) c);
            }
        }
        return sb.toString();
    }

    // Helper method to parse command line into arguments
    private static List<String> parseCommandLine(String commandLine) {
        List<String> args = new ArrayList<>();
        StringBuilder currentArg = new StringBuilder();
        boolean inQuotes = false;

        for (char c : commandLine.toCharArray()) {
            if (c == ' ' && !inQuotes) {
                if (!currentArg.isEmpty()) {
                    args.add(currentArg.toString());
                    currentArg.setLength(0);
                }
            } else if (c == '"') {
                inQuotes = !inQuotes;
            } else {
                currentArg.append(c);
            }
        }

        if (!currentArg.isEmpty()) {
            args.add(currentArg.toString());
        }

        return args;
    }

    private static double calculatePercentile(double percentile) {
        if (RedisClient.responseTimes.isEmpty()) {
            return 0.0;
        }

        List<Long> sortedValues = new ArrayList<>(RedisClient.responseTimes);
        Collections.sort(sortedValues);

        int index = (int) Math.ceil(percentile / 100.0 * sortedValues.size()) - 1;
        return sortedValues.get(Math.max(0, index));
    }
}