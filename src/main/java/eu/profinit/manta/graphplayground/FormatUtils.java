package eu.profinit.manta.graphplayground;

import java.text.CharacterIterator;
import java.text.NumberFormat;
import java.text.StringCharacterIterator;

/**
 * Various method for converting several formats
 *
 * @author dbucek
 */
public final class FormatUtils {
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance();

    private FormatUtils() {
    }

    /**
     * Converts CamelCase string to UPPER_SNAKE_CASE string
     *
     * @param camelCase input camel case
     * @return Snake Case
     */
    public static String convertCamelToSnake(final String camelCase) {
        // Empty String
        StringBuilder result = new StringBuilder();

        // Append first character(in lower case)
        // to result string
        char c = camelCase.charAt(0);
        result.append(Character.toLowerCase(c));

        // Traverse the string from
        // ist index to last index
        for (int i = 1; i < camelCase.length(); i++) {

            char ch = camelCase.charAt(i);

            // Check if the character is upper case
            // then append '_' and such character
            // (in lower case) to result string
            if (Character.isUpperCase(ch)) {
                result.append('_');
                result.append(Character.toLowerCase(ch));
            }

            // If the character is lower case then
            // add such character into result string
            else {
                result.append(ch);
            }
        }

        // return the result
        return result.toString().toUpperCase();
    }

    /**
     * Format bytes into human readable string
     *
     * @param bytes number of bytes in long
     * @return Formatted string
     */
    public static String humanReadableByteCountSI(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.1f %cB", bytes / 1000.0, ci.current());
    }

    public static String humanReadableByteCountSI(Number bytes) {
        return humanReadableByteCountSI(bytes.longValue());
    }

    /**
     * Format nanoseconds into human readable time
     * <p>
     * Always show three values. If days are not zero then Xd Yh Zm, then If hours are not zero then Xh Ym Zs, else
     * Xm Ys Zms.
     *
     * @param nanos Input in nanoseconds
     * @return Formatted string
     */
    public static String humanReadableTime(long nanos) {
        // ms
        long msWhole = nanos / 1_000_000;
        double msDecimal = (nanos / 1_000_000f) - msWhole;
        double ms = (msWhole % 1000) + msDecimal;

        long tempSec = nanos / (1000 * 1000 * 1000);

        long sec = tempSec % 60;
        long min = (tempSec / 60) % 60;
        long hour = (tempSec / (60 * 60)) % 24;
        long day = (tempSec / (24 * 60 * 60)) % 24;

        if (day > 0) {
            return String.format("%dd %dh %dm", day, hour, min);
        } else if (hour > 0) {
            return String.format("%dh %dm %ds", hour, min, sec);
        } else {
            return String.format("%dm %ds %fms", min, sec, ms);
        }
    }

    /**
     * Human readable long with separators
     *
     * @param longNumber Number
     * @return Formatted number
     */
    public static String humanReadableNumber(long longNumber) {
        return NUMBER_FORMAT.format(longNumber);
    }

    /**
     * Human readable double with separators
     *
     * @param doubleNumber Number
     * @return Formatted number
     */
    public static String humanReadableNumber(double doubleNumber) {
        return NUMBER_FORMAT.format(doubleNumber);
    }

    /**
     * Human readable double with separators
     *
     * @param number Number
     * @return Formatted number
     */
    public static String humanReadableNumber(Number number) {
        if (number instanceof Long) {
            return NUMBER_FORMAT.format(number.longValue());
        } else if (number instanceof Double) {
            return NUMBER_FORMAT.format(number.longValue());
        } else {
            throw new RuntimeException("Unknown number type");
        }
    }
}