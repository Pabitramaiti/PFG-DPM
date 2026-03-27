package com.broadridge.ebcdic;

public class JsonPrettyFormat {
   public static String toPrettyFormat(String jsonString) {
        StringBuilder prettyJson = new StringBuilder();
        int indentLevel = 0;
        boolean inQuote = false;

        for (char charFromJson : jsonString.toCharArray()) {
            switch (charFromJson) {
                case '"':
                    // switch the quoting status
                    inQuote = !inQuote;
                    prettyJson.append(charFromJson);
                    break;
                case ' ':
                    // For space: ignore the space if it is not in quotes
                    if (inQuote) {
                        prettyJson.append(charFromJson);
                    }
                    break;
                case '{':
                case '[':
                    // Opening brackets should increase the indent level
                    prettyJson.append(charFromJson).append("\n");
                    indentLevel++;
                    appendIndent(prettyJson, indentLevel);
                    break;
                case '}':
                case ']':
                    // Closing brackets should decrease the indent level
                    prettyJson.append("\n");
                    indentLevel--;
                    appendIndent(prettyJson, indentLevel);
                    prettyJson.append(charFromJson);
                    break;
                case ',':
                    // Commas should add a newline and indentation
                    // Commas should add a newline and indentation
                    prettyJson.append(charFromJson);
                    if (!inQuote) {
                        prettyJson.append("\n");
                        appendIndent(prettyJson, indentLevel);
                    }
//                    prettyJson.append(charFromJson).append("\n");
//                    appendIndent(prettyJson, indentLevel);
                    break;
                case ':':
                    // Colons should add a space after them
                    prettyJson.append(charFromJson).append(" ");
                    break;
                default:
                    prettyJson.append(charFromJson);
                    break;
            }
        }

        return prettyJson.toString();
    }

    private static void appendIndent(StringBuilder stringBuilder, int indentLevel) {
        for (int i = 0; i < indentLevel; i++) {
            stringBuilder.append("    "); // Using 4 spaces for indentation
        }
    }

}
