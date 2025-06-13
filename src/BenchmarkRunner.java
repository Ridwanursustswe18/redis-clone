import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class BenchmarkRunner {
    private final BenchmarkStats stats;

    public BenchmarkRunner() {
        this.stats = new BenchmarkStats();
    }

    public BenchmarkResult runBenchmark(BenchmarkConfig config) {
        config.print();

        ExecutorService executor = Executors.newFixedThreadPool(config.numClients());
        CountDownLatch latch = new CountDownLatch(config.numClients());
        long startTime = System.currentTimeMillis();

        stats.reset();

        for (int i = 0; i < config.numClients(); i++) {
            final int clientId = i;
            executor.submit(() -> {
                try {
                    runClientBenchmark(clientId, config);
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

            return stats.getResults(totalTimeSeconds, config.getTotalRequests());
        } catch (InterruptedException e) {
            System.err.println("Benchmark interrupted: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            executor.shutdown();
        }
    }

    private void runClientBenchmark(int clientId, BenchmarkConfig config) {
        try (RedisConnection connection = new RedisConnection(config.host(), config.port())) {

            for (int i = 0; i < config.requestsPerClient(); i++) {
                try {
                    long startTime = System.nanoTime();
                    connection.executeCommand(config.command());
                    long endTime = System.nanoTime();

                    long latencyMs = (endTime - startTime) / 1_000_000;
                    stats.recordSuccess(latencyMs);
                } catch (Exception e) {
                    stats.recordError();
                }
            }
        } catch (Exception e) {
            System.err.println("Client " + clientId + " connection error: " + e.getMessage());
        }
    }
}