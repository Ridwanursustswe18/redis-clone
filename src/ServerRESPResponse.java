import java.io.IOException;
import java.io.OutputStream;

public class ServerRESPResponse {
    public void sendSimpleString(OutputStream out, String str) throws IOException {
        out.write(('+' + str + "\r\n").getBytes());
    }

    public void sendError(OutputStream out, String str) throws IOException {
        out.write(('-' + str + "\r\n").getBytes());
    }

    public void sendInteger(OutputStream out, long value) throws IOException {
        out.write((':' + String.valueOf(value) + "\r\n").getBytes());
    }

    public void sendBulkString(OutputStream out, String str) throws IOException {
        byte[] data = str.getBytes();
        out.write(('$' + String.valueOf(data.length) + "\r\n").getBytes());
        out.write(data);
        out.write("\r\n".getBytes());
    }

    public void sendNullBulkString(OutputStream out) throws IOException {
        out.write("$-1\r\n".getBytes());
    }
}
