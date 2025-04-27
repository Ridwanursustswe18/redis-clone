import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {
    private static final int PORT = 6379;
    private static final Map<String, String> dataStore = new HashMap<>();
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(10);

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

    private static void executeCommand(String[] command, OutputStream outputStream) throws IOException {
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
                    sendSimpleString(outputStream, "OK");
                }
                break;

            case "GET":
                if (command.length < 2) {
                    sendError(outputStream, "ERR wrong number of arguments for 'GET' command");
                } else {
                    String value = dataStore.get(command[1]);
                    if (value != null) {
                        sendBulkString(outputStream, value);
                    } else {
                        sendNullBulkString(outputStream);
                    }
                }
                break;

            case "DEL":
                if (command.length < 2) {
                    sendError(outputStream, "ERR wrong number of arguments for 'DEL' command");
                } else {
                    int count = 0;
                    for (int i = 1; i < command.length; i++) {
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
                        if (dataStore.containsKey(command[i])) {
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