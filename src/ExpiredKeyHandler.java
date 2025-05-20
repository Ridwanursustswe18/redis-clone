import java.util.*;

public class ExpiredKeyHandler {
    private static final double TRIGGER_PROBABILITY = 0.10;
    private static final int KEYS_TO_SAMPLE = 10;
    private static final double CONTINUE_THRESHOLD = 0.25;
    public void probabilisticKeyExpiration() {
        // Only run the sampling with a 10% probability
        if (Math.random() >= TRIGGER_PROBABILITY) {
            return;
        }
        // Continue sampling as long as we're finding many expired keys
        boolean continueChecking;
        int maxRounds = 20;
        do {
            continueChecking = sampleAndExpireKeys();
            maxRounds--;
        } while (continueChecking && maxRounds > 0);
    }

    private static boolean sampleAndExpireKeys() {
        // Get up to KEYS_TO_SAMPLE random keys with expiry times
        List<String> keysWithExpiry = new ArrayList<>(RedisServer.keyExpiryTimes.keySet());
        if (keysWithExpiry.isEmpty()) {
            return false;
        }

        Collections.shuffle(keysWithExpiry);
        int sampleSize = Math.min(KEYS_TO_SAMPLE, keysWithExpiry.size());
        List<String> sampledKeys = keysWithExpiry.subList(0, sampleSize);
        int expiredCount = 0;
        long now = System.currentTimeMillis();
        for (String key : sampledKeys) {
            Long expiryTime = RedisServer.keyExpiryTimes.get(key);
            if (expiryTime != null && now > expiryTime) {
                RedisServer.dataStore.remove(key);
                RedisServer.keyExpiryTimes.remove(key);
                expiredCount++;
            }
        }

        // If more than 25% of keys were expired, signal to continue sampling
        return (double) expiredCount / sampleSize > CONTINUE_THRESHOLD;
    }
    public boolean isKeyExpired(String key) {
        Long expiryTime = RedisServer.keyExpiryTimes.get(key);
        if (expiryTime == null) {
            return false;
        }
        return System.currentTimeMillis() > expiryTime;
    }

    public void removeExpiredKey(String key) {
        RedisServer.dataStore.remove(key);
        RedisServer.keyExpiryTimes.remove(key);
    }
}
