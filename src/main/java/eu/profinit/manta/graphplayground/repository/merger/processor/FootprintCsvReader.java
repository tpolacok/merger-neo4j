package eu.profinit.manta.graphplayground.repository.merger.processor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

/**
 * CSV reader storing the read lines to footprint to make revisiting possible.
 * <br/>
 * There are three states:
 * <ul>
 *   <li>Open footprint - line read from stream and stored into footpring</li>
 *   <li>Closed unread footprint - line read from footprint</li>
 *   <li>Closed read footprint - line read from stream but not stored into footprint</li>
 * </ul>
 */
public final class FootprintCsvReader {


    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(FootprintCsvReader.class);

    /**
     * {@link CSVReader} instance used to read from stream.
     */
    private final CSVReader csvReader;

    /**
     * Footprint containing read lines.
     */
    private final List<String[]> footprint = new LinkedList<>();

    /**
     * Flag of closed footprint.
     */
    private boolean footprintClosed;

    /**
     * Flag of read footprint.
     */
    private boolean footprintRead;

    /**
     * Flag of read input stream.
     */
    private boolean streamRead;

    /**
     * Footprint iterator.
     */
    private Iterator<String[]> footprintIterator;

    /**
     * @param csvReader {@link CSVReader} instance used to read from stream.
     */
    public FootprintCsvReader(CSVReader csvReader) {
        this.csvReader = csvReader;
    }
    
    /**
     * Reads the next row. The way of reading depends on a reader state - for more information see the class
     * documentation {@link FootprintCsvReader}.
     * @return non-empty String[] - read row
     *         empty String[] - the case of all attributes omitted because of their invalidity
     *                          (check {@link FootprintCsvReader#getValidArguments(String[])}
     *         null - end of csv reader
     * @throws IOException Exception from row reading of the original stream
     */
    public String[] readNext() throws IOException, CsvValidationException {
        String[] validLine;

        if (footprintClosed) {
            if (footprintIterator.hasNext()) {
                validLine = getValidArguments(footprintIterator.next());
                return validLine;
            }
            footprintRead = true;
            if (streamRead) {
                return null;
            }
            String[] line = csvReader.readNext();
            if (line == null) {
                streamRead = true;
                return null;
            }
            validLine = getValidArguments(line);
            return validLine;
        }
        // footprintClosed is false
        if (streamRead) {
            return null;
        }
        String[] line = csvReader.readNext();
        if (line == null) {
            streamRead = true;
            return null;
        }
        validLine = getValidArguments(line);
        footprint.add(validLine);
        return validLine;
    }

    /**
     * Checks if each argument meets size restrictions of the Persistit storage backend.
     * If an argument is too large and doesn't fit the storage, it is omitted and
     * a corresponding warning message is logged.
     * @param arguments String array containing individual arguments to be stored
     * @return String array of only valid arguments, null if no argument is valid
     */
    private String[] getValidArguments(String[] arguments) {
        List<String> validArguments = new ArrayList<>();

        for (String argument : arguments) {
            if (argument.getBytes(Charset.defaultCharset()).length > 4194304) {
                LOGGER.warn("Omitted argument for its exceeded maximum allowed size! " +
                        "Maximum allowed size = " + 4194304 + " bytes. " +
                        "Current argument size = " + argument.getBytes(Charset.defaultCharset()).length + " bytes. " +
                        "The first 1000 chars of the argument:\n" + argument.substring(0, Math.min(1000, argument.length()-1)));
            } else {
                validArguments.add(argument);
            }
        }

        return validArguments.toArray(new String[0]);
    }

    /**
     * Closes the footprint.
     */
    public void closeFootprint() {
        this.footprintClosed = true;
        footprintIterator = footprint.iterator();
    }

    /**
     * @return Flag of read footprint.
     */
    public boolean isFootprintRead() {
        return footprintRead;
    }
}