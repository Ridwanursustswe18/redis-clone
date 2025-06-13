import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collects and calculates benchmark statistics
 */
class BenchmarkStats {
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final List<Long> responseTimes = Collections.synchronizedList(new ArrayList<>());

    public void recordSuccess(long responseTimeMs) {
        successCount.incrementAndGet();
        responseTimes.add(responseTimeMs);
    }

    public void recordError() {
        errorCount.incrementAndGet();
    }

    public void reset() {
        successCount.set(0);
        errorCount.set(0);
        responseTimes.clear();
    }

    public BenchmarkResult getResults(double totalTimeSeconds, int totalRequests) {
        double requestsPerSecond = totalRequests / totalTimeSeconds;
        double p50Latency = calculatePercentile(50);

        return new BenchmarkResult(
                totalTimeSeconds,
                totalRequests,
                successCount.get(),
                errorCount.get(),
                requestsPerSecond,
                p50Latency
        );
    }

    private double calculatePercentile(double percentile) {
        if (responseTimes.isEmpty()) {
            return 0.0;
        }

        List<Long> sortedValues = new ArrayList<>(responseTimes);
        Collections.sort(sortedValues);

        int index = (int) Math.ceil(percentile / 100.0 * sortedValues.size()) - 1;
        return sortedValues.get(Math.max(0, index));
    }
}
