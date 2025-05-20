import java.io.*;
import java.net.Socket;

public class ClientHandler {
    private static final ServerRESPResponse serverRESPResponse = new ServerRESPResponse();
    private final CommandExecutor commandExecutor;

    public ClientHandler(CommandExecutor commandExecutor) {

        this.commandExecutor = commandExecutor;
    }

    public void handleClient(Socket clientSocket) {
        try (
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))
        ) {
            while (!clientSocket.isClosed()) {
                String arrayHeader = reader.readLine();
                if (arrayHeader == null) break;


                if (!arrayHeader.startsWith("*")) {
                    serverRESPResponse.sendError(outputStream, "Protocol error: expected '*'");
                    continue;
                }

                int numArgs;
                try {
                    numArgs = Integer.parseInt(arrayHeader.substring(1));
                } catch (NumberFormatException e) {
                    serverRESPResponse.sendError(outputStream, "Protocol error: invalid array size");
                    continue;
                }

                String[] command = new String[numArgs];
                for (int i = 0; i < numArgs; i++) {
                    String bulkHeader = reader.readLine();
                    if (bulkHeader == null || !bulkHeader.startsWith("$")) {
                        serverRESPResponse.sendError(outputStream, "Protocol error: expected '$'");
                        continue;
                    }

                    int strLen;
                    try {
                        strLen = Integer.parseInt(bulkHeader.substring(1));
                    } catch (NumberFormatException e) {
                        serverRESPResponse.sendError(outputStream, "Protocol error: invalid string length");
                        continue;
                    }

                    char[] buffer = new char[strLen];
                    reader.read(buffer, 0, strLen);
                    command[i] = new String(buffer);
                    reader.readLine(); // Consume CRLF
                }

                commandExecutor.executeCommand(command, outputStream);
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
}
