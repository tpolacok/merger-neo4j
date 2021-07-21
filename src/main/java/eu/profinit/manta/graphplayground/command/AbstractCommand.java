package eu.profinit.manta.graphplayground.command;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Map;

/**
 * Superclass of any command you want to implement
 *
 * @author dbucek
 */
public abstract class AbstractCommand {
    /**
     * Main entry point - argument will supply from the main class
     *
     * @param options args supplied by graph bench
     * @throws Exception Any possible checked exception
     */
    public abstract void run(Map<String, Option> options) throws Exception;

    /**
     * Define OptionGroup which need command on command line
     *
     * @return Defined OptionGroup
     */
    public abstract Options getOptions();
}
