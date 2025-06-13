import java.util.List;
record BenchmarkConfig(
        String host,
        int port,
        int numClients,
        int requestsPerClient,
        List<String> command
) {
    public int getTotalRequests() {
        return numClients * requestsPerClient;
    }

    public void print() {
        System.out.println("\nStarting benchmark with:");
        System.out.println("- " + numClients + " concurrent clients");
        System.out.println("- " + requestsPerClient + " requests per client");
        System.out.println("- Command: " + String.join(" ", command));
        System.out.println("- Total requests: " + getTotalRequests());
    }
}