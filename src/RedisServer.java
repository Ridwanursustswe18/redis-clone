import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {
    private static final int PORT = 6379;
    private static final Map<String, String> dataStore = new HashMap<>();
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(50);
    private static final Map<String, Long> keyExpiryTimes = new HashMap<>();
    private static final double TRIGGER_PROBABILITY = 0.10;
    private static final int KEYS_TO_SAMPLE = 10;
    private static final double CONTINUE_THRESHOLD = 0.25;

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Redis clone server started on port " + PORT);

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Client connected: " + clientSocket.getInetAddress());
                    threadPool.execute(() -> handleClient(clientSocket));
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
            threadPool.shutdown();
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))
        ) {
            while (!clientSocket.isClosed()) {
                String arrayHeader = reader.readLine();
                System.out.println(arrayHeader);
                if (arrayHeader == null) break;

                if (!arrayHeader.startsWith("*")) {
                    sendError(outputStream, "Protocol error: expected '*'");
                    continue;
                }

                int numArgs;
                try {
                    numArgs = Integer.parseInt(arrayHeader.substring(1));
                } catch (NumberFormatException e) {
                    sendError(outputStream, "Protocol error: invalid array size");
                    continue;
                }

                String[] command = new String[numArgs];
                for (int i = 0; i < numArgs; i++) {
                    String bulkHeader = reader.readLine();
                    if (bulkHeader == null || !bulkHeader.startsWith("$")) {
                        sendError(outputStream, "Protocol error: expected '$'");
                        continue;
                    }

                    int strLen;
                    try {
                        strLen = Integer.parseInt(bulkHeader.substring(1));
                    } catch (NumberFormatException e) {
                        sendError(outputStream, "Protocol error: invalid string length");
                        continue;
                    }

                    char[] buffer = new char[strLen];
                    reader.read(buffer, 0, strLen);
                    command[i] = new String(buffer);
                    reader.readLine(); // Consume CRLF
                }

                executeCommand(command, outputStream);
            }
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
    private static void probabilisticKeyExpiration() {
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
        List<String> keysWithExpiry = new ArrayList<>(keyExpiryTimes.keySet());
        if (keysWithExpiry.isEmpty()) {
            return false;
        }

        Collections.shuffle(keysWithExpiry);
        int sampleSize = Math.min(KEYS_TO_SAMPLE, keysWithExpiry.size());
        List<String> sampledKeys = keysWithExpiry.subList(0, sampleSize);
        int expiredCount = 0;
        long now = System.currentTimeMillis();
        for (String key : sampledKeys) {
            Long expiryTime = keyExpiryTimes.get(key);
            if (expiryTime != null && now > expiryTime) {
                dataStore.remove(key);
                keyExpiryTimes.remove(key);
                expiredCount++;
            }
        }

        // If more than 25% of keys were expired, signal to continue sampling
        return (double) expiredCount / sampleSize > CONTINUE_THRESHOLD;
    }
    private static boolean isKeyExpired(String key) {
        Long expiryTime = keyExpiryTimes.get(key);
        if (expiryTime == null) {
            return false;
        }
        return System.currentTimeMillis() > expiryTime;
    }

    private static void removeExpiredKey(String key) {
        dataStore.remove(key);
        keyExpiryTimes.remove(key);
    }

    private static void executeCommand(String[] command, OutputStream outputStream) throws IOException {
        probabilisticKeyExpiration();
        if (command.length == 0) {
            sendError(outputStream, "ERR no command specified");
            return;
        }

        String cmd = command[0].toUpperCase();
        switch (cmd) {
            case "PING":
                sendSimpleString(outputStream, "PONG");
                break;

            case "ECHO":
                if (command.length < 2) {
                    sendError(outputStream, "ERR wrong number of arguments for 'ECHO' command");
                } else {
                    sendBulkString(outputStream, command[1]);
                }
                break;

            case "SET":
                if (command.length < 3) {
                    sendError(outputStream, "ERR wrong number of arguments for 'SET' command");
                } else {
                    dataStore.put(command[1], command[2]);
                    if (command.length >= 5) {
                        String option = command[3].toUpperCase();
                        try {
                            long expiry = Long.parseLong(command[4]);
                            if (option.contains("EX") || option.contains("EAXT")) {
                                keyExpiryTimes.put(command[1], System.currentTimeMillis() + (expiry * 1000));
                            }else {
                                keyExpiryTimes.put(command[1], System.currentTimeMillis() + expiry);
                            }
                        } catch (NumberFormatException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    sendSimpleString(outputStream, "OK");
                }
                break;

            case "GET":
                if (command.length < 2) {
                    sendError(outputStream, "ERR wrong number of arguments for 'GET' command");
                } else {
                    String key = command[1];
                    if (isKeyExpired(key)) {
                        removeExpiredKey(key);
                        sendNullBulkString(outputStream);
                    } else {
                        String value = dataStore.get(key);
                        if (value != null) {
                            sendBulkString(outputStream, value);
                        } else {
                            sendNullBulkString(outputStream);
                        }
                    }
                }
                break;

            case "DEL":
                if (command.length < 2) {
                    sendError(outputStream, "ERR wrong number of arguments for 'DEL' command");
                } else {
                    int count = 0;
                    for (int i = 1; i < command.length; i++) {
                        String key = command[i];
                        if (isKeyExpired(key)) {
                            removeExpiredKey(key);
                            sendNullBulkString(outputStream);
                        }
                        if (dataStore.remove(command[i]) != null) {
                            count++;
                        }
                    }
                    sendInteger(outputStream, count);
                }
                break;

            case "EXISTS":
                if (command.length < 2) {
                    sendError(outputStream, "ERR wrong number of arguments for 'EXISTS' command");
                } else {
                    int count = 0;
                    for (int i = 1; i < command.length; i++) {
                        String key = command[i];
                        if (isKeyExpired(key)) {
                            removeExpiredKey(key);
                            sendNullBulkString(outputStream);
                        }
                        if (dataStore.containsKey(key)){
                            count++;
                        }
                    }
                    sendInteger(outputStream, count);
                }
                break;

            default:
                sendError(outputStream, "ERR unknown command '" + cmd + "'");
        }
    }


    private static void sendSimpleString(OutputStream out, String str) throws IOException {
        out.write(('+' + str + "\r\n").getBytes());
    }

    private static void sendError(OutputStream out, String str) throws IOException {
        out.write(('-' + str + "\r\n").getBytes());
    }

    private static void sendInteger(OutputStream out, long value) throws IOException {
        out.write((':' + String.valueOf(value) + "\r\n").getBytes());
    }

    private static void sendBulkString(OutputStream out, String str) throws IOException {
        byte[] data = str.getBytes();
        out.write(('$' + String.valueOf(data.length) + "\r\n").getBytes());
        out.write(data);
        out.write("\r\n".getBytes());
    }

    private static void sendNullBulkString(OutputStream out) throws IOException {
        out.write("$-1\r\n".getBytes());
    }
}