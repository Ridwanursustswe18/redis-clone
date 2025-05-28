import java.io.*;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CommandExecutor {
    private final ExpiredKeyHandler expiredKeyHandler;
    private final ServerRESPResponse serverRESPResponse;
    private final KeyPersistenceService keyPersistenceService;

    public CommandExecutor(ExpiredKeyHandler expiredKeyHandler, ServerRESPResponse serverRESPResponse, KeyPersistenceService keyPersistenceService) {
        this.expiredKeyHandler = expiredKeyHandler;
        this.serverRESPResponse = serverRESPResponse;
        this.keyPersistenceService = keyPersistenceService;
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
                    RedisServer.numberOfKeysChanged++;
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
                            RedisServer.numberOfKeysChanged++;
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
                    RedisServer.numberOfKeysChanged++;
                    String key = command[1];
                    if (expiredKeyHandler.isKeyExpired(key)) {
                        expiredKeyHandler.removeExpiredKey(key);
                        RedisServer.dataStore.put(key,"1");
                        serverRESPResponse.sendInteger(outputStream, 1);
                    }else if(RedisServer.dataStore.containsKey(key)){
                        String val =  RedisServer.dataStore.get(key);
                        if (isWholeStringInteger(val)){
                            int newVal = Integer.parseInt(val) + 1;
                            RedisServer.dataStore.put(key,String.valueOf(newVal));
                            serverRESPResponse.sendInteger(outputStream, newVal);
                        }else{
                            RedisServer.numberOfKeysChanged--;
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
                    RedisServer.numberOfKeysChanged++;
                    String key = command[1];
                    if (expiredKeyHandler.isKeyExpired(key)) {
                        expiredKeyHandler.removeExpiredKey(key);
                        RedisServer.dataStore.put(key,"0");
                        serverRESPResponse.sendInteger(outputStream, 0);
                    }else if(RedisServer.dataStore.containsKey(key)){
                        String val =  RedisServer.dataStore.get(key);
                        if (isWholeStringInteger(val)){
                            int newVal = Integer.parseInt(val) - 1;
                            RedisServer.dataStore.put(key,String.valueOf(newVal));
                            serverRESPResponse.sendInteger(outputStream, newVal);
                        }else{
                            RedisServer.numberOfKeysChanged--;
                            serverRESPResponse.sendError(outputStream, "(error) ERR value is not an integer or out of range");
                        }

                    }else{
                        RedisServer.dataStore.put(key,"0");
                        serverRESPResponse.sendInteger(outputStream, 0);
                    }

                }
                break;

            case "LPUSH":
                if (command.length < 3) {
                    serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'LPUSH' command");
                }else{
                    RedisServer.numberOfKeysChanged++;
                    String key = command[1];
                    int count;
                    if(RedisServer.listDataStore.containsKey(key)){
                        LinkedList<String> currList = RedisServer.listDataStore.get(key);
                        for (int i = 2; i < command.length; i++){
                            currList.addFirst(command[i]);
                        }
                        count = currList.size();
                        RedisServer.listDataStore.put(key, currList);
                    }else {
                        LinkedList<String> list = new LinkedList<>();
                        for (int i = 2; i < command.length; i++) {
                            list.addFirst(command[i]);
                        }
                        count = list.size();
                        RedisServer.listDataStore.put(key, list);
                    }
                    serverRESPResponse.sendInteger(outputStream, count);
                }
                break;

            case "RPUSH":
                if (command.length < 3) {
                    serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'RPUSH' command");
                }else{
                    RedisServer.numberOfKeysChanged++;
                    String key = command[1];
                    int count;
                    if(RedisServer.listDataStore.containsKey(key)){
                        LinkedList<String> currList = RedisServer.listDataStore.get(key);
                        for (int i = 2; i < command.length; i++){
                            currList.addLast(command[i]);
                        }
                        count = currList.size();
                        RedisServer.listDataStore.put(key, currList);
                    }else {
                        LinkedList<String> list = new LinkedList<>();
                        for (int i = 2; i < command.length; i++) {
                            list.addLast(command[i]); // Fixed: was addFirst, should be addLast for RPUSH
                        }
                        count = list.size();
                        RedisServer.listDataStore.put(key, list);
                    }
                    serverRESPResponse.sendInteger(outputStream, count);
                }
                break;

            case "SAVE":
                try {
                    if (command.length == 1) {
                        keyPersistenceService.saveDataToFile("dump.rdb");
                        serverRESPResponse.sendSimpleString(outputStream, "OK");

                    } else if (command.length == 3) {
                        long intervalSeconds = Long.parseLong(command[1]);
                        long minimumKeys = Long.parseLong(command[2]);

                        if (intervalSeconds <= 0 || minimumKeys < 0) {
                            serverRESPResponse.sendError(outputStream, "ERR invalid save parameters");
                            break;
                        }

                        long intervalMs = intervalSeconds * 1000;
                        keyPersistenceService.startBackgroundSave(intervalMs, minimumKeys);
                        serverRESPResponse.sendSimpleString(outputStream, "OK");

                    } else {
                        serverRESPResponse.sendError(outputStream, "ERR wrong number of arguments for 'SAVE' command");
                    }
                } catch (NumberFormatException e) {
                    serverRESPResponse.sendError(outputStream, "ERR invalid number format");
                }
                break;

            case "BGSAVE":
                try {
                    Thread bgSaveThread = new Thread(() -> keyPersistenceService.saveDataToFile("dump.rdb"));
                    bgSaveThread.setDaemon(true);
                    bgSaveThread.start();
                    serverRESPResponse.sendSimpleString(outputStream, "Background saving started");
                } catch (Exception e) {
                    serverRESPResponse.sendError(outputStream, "ERR " + e.getMessage());
                }
                break;

            default:
                serverRESPResponse.sendError(outputStream, "ERR unknown command '" + cmd + "'");
        }
    }

    private boolean isWholeStringInteger(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}