record BenchmarkResult(
        double totalTimeSeconds,
        int totalRequests,
        int successfulRequests,
        int failedRequests,
        double requestsPerSecond,
        double p50LatencyMs
) {
    public void print() {
        System.out.println("\nBenchmark Results:");
        System.out.println("=================");
        System.out.println("Total time: " + totalTimeSeconds + " seconds");
        System.out.println("Total requests: " + totalRequests);
        System.out.println("Successful requests: " + successfulRequests);
        System.out.println("Failed requests: " + failedRequests);
        System.out.println("Requests per second: " + String.format("%.2f", requestsPerSecond));
        System.out.println("p50 latency: " + String.format("%.3f", p50LatencyMs) + " msec");
    }
}