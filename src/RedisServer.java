import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {
    private static final int PORT = 6379;
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(50);
    static final Map<String, String> dataStore = new HashMap<>();
    static final Map<String, Long> keyExpiryTimes = new HashMap<>();
    private static final ServerRESPResponse serverRESPResponse = new ServerRESPResponse();
    private static final  ExpiredKeyHandler expiredKeyHandler = new ExpiredKeyHandler();
    private static final CommandExecutor commandExecutor = new CommandExecutor(expiredKeyHandler,serverRESPResponse);
    private static final ClientHandler clientHandler = new ClientHandler(commandExecutor);

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Redis clone server started on port " + PORT);

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Client connected: " + clientSocket.getInetAddress());
                    threadPool.execute(() -> clientHandler.handleClient(clientSocket));
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
            threadPool.shutdown();
        }
    }
}