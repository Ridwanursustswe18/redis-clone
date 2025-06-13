import java.io.*;
import java.net.Socket;
import java.util.List;

class RedisConnection implements AutoCloseable {
    private final Socket socket;
    private final OutputStream output;
    private final InputStream input;

    public RedisConnection(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.output = socket.getOutputStream();
        this.input = socket.getInputStream();
    }

    public Object executeCommand(List<String> commandArgs) throws IOException {
        byte[] serializedCommand = RedisProtocol.serializeCommand(commandArgs);
        output.write(serializedCommand);
        output.flush();
        return RedisProtocol.deserializeResponse(input);
    }

    @Override
    public void close() throws IOException {
        if (input != null) input.close();
        if (output != null) output.close();
        if (socket != null) socket.close();
    }
}