import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class RedisClient {

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
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        int prefix = reader.read();
        if (prefix == -1) throw new IOException("Unexpected end of stream");

        return switch ((char) prefix) {
            case '+' -> readSimpleString(reader);
            case '-' -> throw new RedisException(readLine(reader));
            case ':' -> Long.parseLong(readLine(reader));
            case '$' -> readBulkString(reader);
            case '*' -> readArray(reader);
            default -> throw new IOException("Unexpected RESP prefix: " + (char) prefix);
        };
    }

    private static String readSimpleString(BufferedReader reader) throws IOException {
        return readLine(reader);
    }

    private static String readBulkString(BufferedReader reader) throws IOException {
        int length = Integer.parseInt(readLine(reader));
        if (length == -1) return null; // Null bulk string
        char[] buffer = new char[length];
        reader.read(buffer, 0, length);
        reader.read(); // Discard CR
        reader.read(); // Discard LF
        return new String(buffer);
    }

    private static Object[] readArray(BufferedReader reader) throws IOException {
        int count = Integer.parseInt(readLine(reader));
        if (count == -1) return null; // Null array
        Object[] array = new Object[count];
        for (int i = 0; i < count; i++) {
            int prefix = reader.read();
            if (prefix == -1) throw new IOException("Unexpected end of stream");

            array[i] = switch ((char) prefix) {
                case '+' -> readSimpleString(reader);
                case '-' -> throw new RedisException(readLine(reader));
                case ':' -> Long.parseLong(readLine(reader));
                case '$' -> readBulkString(reader);
                case '*' -> readArray(reader);
                default -> throw new IOException("Unexpected RESP prefix: " + (char) prefix);
            };
        }
        return array;
    }

    private static String readLine(BufferedReader reader) throws IOException {
        return reader.readLine();
    }

    public static class RedisException extends RuntimeException {
        public RedisException(String message) {
            super("Redis error: " + message);
        }
    }
    public static void main(String[] args) throws IOException {
        try (Socket socket = new Socket("localhost", 6379);
             OutputStream output = socket.getOutputStream();
             InputStream input = socket.getInputStream()) {
            System.out.println("Connected to Redis server on localhost:6379");
            System.out.println("Enter Redis commands (type 'exit' to quit):");

            Scanner scanner = new Scanner(System.in);
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
                    byte[] serializedCommand = serializeCommand(commandArgs);
                    output.write(serializedCommand);
                    output.flush();
                    String response = (String) deserializeResponse(input);
                    System.out.println("Response: " + response);
                } catch (Exception e) {
                    System.out.println("Error executing command: " + e.getMessage());
                }
            }
        }
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
    }}