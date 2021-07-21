package eu.profinit.manta.graphplayground.model.manta.util;

import java.io.Reader;
import java.io.Writer;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

/**
 * Helper pro práci s CSV.
 *
 * @author tfechtner
 * @author onouza
 */
public final class CsvHelper {

    private CsvHelper() {
    }

    /** Defaultní oddělovač používaný ve vstupu. */
    public static final char DELIMITER = ',';
    /** Defaultní znak pro escapování special znaků. */
    public static final char ESCAPE_CHAR = '\\';
    /** Defaultní znak pro "uvozovkování" jmen se speciálními znaky. */
    public static final char QUOTE_CHAR = '"';

    /**
     * Line ending.
     */
    public static final String LINE_END = "\n";

    /**
     * Vytvoří CSV reader s příslušným nakonfigurováním.
     * @param reader vstupní reader s daty pro čtení
     * @return nakonfigurovaný csv reader
     */
    public static CSVReader getCSVReader(Reader reader) {
        return new CSVReader(reader);
    }

    /**
     * Vytvoří CSV writer s příslušným nakonfigurováním.
     * @param writer podkladový writer, kam se zapisují data
     * @return nakonfigurovaný csv writer
     */
    public static CSVWriter getCSVWriter(Writer writer) {
        return new CSVWriter(writer, DELIMITER, QUOTE_CHAR, ESCAPE_CHAR, LINE_END);
    }
}
