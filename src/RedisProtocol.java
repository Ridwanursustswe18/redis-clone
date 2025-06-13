import java.io.IOException;
import java.io.InputStream;
import java.util.List;

class RedisProtocol {

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
            case '-' -> "Error: " + readLine(inputStream);
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
        if (length == -1) return null;

        byte[] buffer = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length) {
            int read = inputStream.read(buffer, bytesRead, length - bytesRead);
            if (read == -1) throw new IOException("Unexpected end of stream");
            bytesRead += read;
        }

        inputStream.read(); // CR
        inputStream.read(); // LF

        return new String(buffer);
    }

    private static Object[] readArray(InputStream inputStream) throws IOException {
        int count = Integer.parseInt(readLine(inputStream));
        if (count == -1) return null;

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
}
