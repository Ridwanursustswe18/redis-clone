import java.io.IOException;
import java.io.OutputStream;

public class CommandExecutor {
    private final ExpiredKeyHandler expiredKeyHandler;
    private final ServerRESPResponse serverRESPResponse ;

    public CommandExecutor(ExpiredKeyHandler expiredKeyHandler, ServerRESPResponse serverRESPResponse) {
        this.expiredKeyHandler = expiredKeyHandler;
        this.serverRESPResponse = serverRESPResponse;
    }

    public void executeCommand(String[] command, OutputStream outputStream) throws IOException {
        expiredKeyHandler.probabilisticKeyExpiration();
        if (command.length == 0) {
            serverRESPResponse.sendError(outputStream, "ERR no command specified");
            return;
        }

        String cmd = command[0].toUpperCase();
        switch (cmd) {
            case "PING":
                serverRESPResponse.sendSimpleString(outputStream, "PONG");
                break;

            case "ECHO":
                if (command.length < 2) {
                    serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'ECHO' command");
                } else {
                    serverRESPResponse.sendBulkString(outputStream, command[1]);
                }
                break;

            case "SET":
                if (command.length < 3) {
                    serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'SET' command");
                } else {
                    RedisServer.dataStore.put(command[1], command[2]);
                    if (command.length >= 5) {
                        String option = command[3].toUpperCase();
                        try {
                            long expiry = Long.parseLong(command[4]);
                            if (option.contains("EX") || option.contains("EAXT")) {
                                RedisServer.keyExpiryTimes.put(command[1], System.currentTimeMillis() + (expiry * 1000));
                            }else {
                                RedisServer.keyExpiryTimes.put(command[1], System.currentTimeMillis() + expiry);
                            }
                        } catch (NumberFormatException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    serverRESPResponse.sendSimpleString(outputStream, "OK");
                }
                break;

            case "GET":
                if (command.length < 2) {
                    serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'GET' command");
                } else {
                    String key = command[1];
                    if (expiredKeyHandler.isKeyExpired(key)) {
                        expiredKeyHandler.removeExpiredKey(key);
                        serverRESPResponse.sendNullBulkString(outputStream);
                    } else {
                        String value = RedisServer.dataStore.get(key);
                        if (value != null) {
                            serverRESPResponse.sendBulkString(outputStream, value);
                        } else {
                            serverRESPResponse.sendNullBulkString(outputStream);
                        }
                    }
                }
                break;

            case "DEL":
                if (command.length < 2) {
                    serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'DEL' command");
                } else {
                    int count = 0;
                    for (int i = 1; i < command.length; i++) {
                        String key = command[i];
                        if (expiredKeyHandler.isKeyExpired(key)) {
                            expiredKeyHandler.removeExpiredKey(key);
                        } else if (RedisServer.dataStore.remove(key) != null) {
                            count++;
                        }
                    }
                    serverRESPResponse.sendInteger(outputStream, count);
                }
                break;

            case "EXISTS":
                if (command.length < 2) {
                    serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'EXISTS' command");
                } else {
                    int count = 0;
                    for (int i = 1; i < command.length; i++) {
                        String key = command[i];
                        if (expiredKeyHandler.isKeyExpired(key)) {
                            expiredKeyHandler.removeExpiredKey(key);
                        } else if (RedisServer.dataStore.containsKey(key)) {
                            count++;
                        }
                    }
                    serverRESPResponse.sendInteger(outputStream, count);
                }
                break;
            case "INCR":
                if (command.length < 2) {
                    serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'INCR' command");
                }else{
                    String key = command[1];
                    if (expiredKeyHandler.isKeyExpired(key)) {
                        expiredKeyHandler.removeExpiredKey(key);
                    }else if(RedisServer.dataStore.containsKey(key)){
                        String val =  RedisServer.dataStore.get(key);
                        if (isWholeStringInteger(val)){
                            int newVal = Integer.parseInt(val) + 1;
                            RedisServer.dataStore.put(key,String.valueOf(newVal));
                            serverRESPResponse.sendInteger(outputStream, newVal);
                        }else{
                            serverRESPResponse.sendError(outputStream, "(error) ERR value is not an integer or out of range");
                        }

                    }else{
                        RedisServer.dataStore.put(key,"1");
                        serverRESPResponse.sendInteger(outputStream, 1);
                    }

                }
                break;
            case "DECR":
                if (command.length < 2) {
                    serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'DECR' command");
                }else{
                    String key = command[1];
                    if (expiredKeyHandler.isKeyExpired(key)) {
                        expiredKeyHandler.removeExpiredKey(key);
                    }else if(RedisServer.dataStore.containsKey(key)){
                        String val =  RedisServer.dataStore.get(key);
                        if (isWholeStringInteger(val)){
                            int newVal = Integer.parseInt(val) - 1;
                            RedisServer.dataStore.put(key,String.valueOf(newVal));
                            serverRESPResponse.sendInteger(outputStream, newVal);
                        }else{
                            serverRESPResponse.sendError(outputStream, "(error) ERR value is not an integer or out of range");
                        }

                    }else{
                        RedisServer.dataStore.put(key,"0");
                        serverRESPResponse.sendInteger(outputStream, 0);
                    }

                }
                break;
            default:
                serverRESPResponse.sendError(outputStream, "ERR unknown command '" + cmd + "'");
        }
    }
    public boolean isWholeStringInteger(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}