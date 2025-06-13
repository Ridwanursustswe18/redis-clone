import java.util.ArrayList;
import java.util.List;

class CommandParser {

    public static List<String> parseCommandLine(String commandLine) {
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
    }
}