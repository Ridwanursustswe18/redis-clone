import java.io.*;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class KeyPersistenceService {
    private static Thread backgroundSaveThread;
    private static final AtomicBoolean saveThreadRunning = new AtomicBoolean(false);
    // Method to save data to file
    public void saveDataToFile(String filename) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
            oos.writeInt(1); // Version
            oos.writeLong(System.currentTimeMillis());

            oos.writeInt(RedisServer.dataStore.size());
            for (Map.Entry<String, String> entry : RedisServer.dataStore.entrySet()) {
                oos.writeUTF("STRING");
                oos.writeUTF(entry.getKey());
                oos.writeUTF(entry.getValue());
            }

            oos.writeInt(RedisServer.listDataStore.size());
            for (Map.Entry<String, LinkedList<String>> entry : RedisServer.listDataStore.entrySet()) {
                oos.writeUTF("LIST");
                oos.writeUTF(entry.getKey());
                oos.writeObject(entry.getValue());
            }

            oos.writeInt(RedisServer.keyExpiryTimes.size());
            for (Map.Entry<String, Long> entry : RedisServer.keyExpiryTimes.entrySet()) {
                oos.writeUTF("EXPIRY");
                oos.writeUTF(entry.getKey());
                oos.writeLong(entry.getValue());
            }

            System.out.println("Data saved to " + filename + " at " + new java.util.Date());

            RedisServer.numberOfKeysChanged = 0;

        } catch (IOException e) {
            System.err.println("Error saving data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void loadDataFromFile(String filename) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename))) {
            // Read version and timestamp
            int version = ois.readInt();
            long timestamp = ois.readLong();

            // Load main dataStore
            int stringStoreSize = ois.readInt();
            for (int i = 0; i < stringStoreSize; i++) {
                String type = ois.readUTF();
                String key = ois.readUTF();
                String value = ois.readUTF();
                RedisServer.dataStore.put(key, value);
            }

            // Load listDataStore
            int listStoreSize = ois.readInt();
            for (int i = 0; i < listStoreSize; i++) {
                String type = ois.readUTF();
                String key = ois.readUTF();
                LinkedList<String> value = (LinkedList<String>) ois.readObject();
                RedisServer.listDataStore.put(key, value);
            }

            // Load key expiry times
            int expirySize = ois.readInt();
            for (int i = 0; i < expirySize; i++) {
                String type = ois.readUTF();
                String key = ois.readUTF();
                long expiryTime = ois.readLong();
                RedisServer.keyExpiryTimes.put(key, expiryTime);
            }

            System.out.println("Data loaded from " + filename + " (saved at " + new java.util.Date(timestamp) + ")");

        } catch (FileNotFoundException e) {
            System.out.println("No existing data file found: " + filename);
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error loading data: " + e.getMessage());
            e.printStackTrace();
        }
    }


    public void startBackgroundSave(long intervalMs, long threshold) {
        stopBackgroundSave();

        saveThreadRunning.set(true);

        backgroundSaveThread = new Thread(() -> {
            while (saveThreadRunning.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(intervalMs);
                    if (RedisServer.numberOfKeysChanged >= threshold) {
                        saveDataToFile("dump.rdb");
                        System.out.println("Background save completed. Keys changed: " + RedisServer.numberOfKeysChanged);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        backgroundSaveThread.setDaemon(true);
        backgroundSaveThread.start();
        System.out.println("Background save started: interval=" + intervalMs + "ms, threshold=" + threshold);
    }

    // Stop background save thread
    private static void stopBackgroundSave() {
        if (backgroundSaveThread != null && saveThreadRunning.get()) {
            saveThreadRunning.set(false);
            backgroundSaveThread.interrupt();
            try {
                backgroundSaveThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
