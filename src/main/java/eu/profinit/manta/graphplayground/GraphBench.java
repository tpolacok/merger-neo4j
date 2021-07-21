package eu.profinit.manta.graphplayground;

import eu.profinit.manta.graphplayground.command.AbstractCommand;
import eu.profinit.manta.graphplayground.profiler.ProfilerAspect;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Main class of Graph Bench
 * <p>
 * See README.md for use
 *
 * @author dbucek
 */
@SpringBootApplication
public class GraphBench implements CommandLineRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphBench.class);

    private static final String ARG_COMMAND = "command";

    private final Map<String, AbstractCommand> commands;
    private final ProfilerAspect profilerAspect;

    public GraphBench(Map<String, AbstractCommand> commands, ProfilerAspect profilerAspect) {
        this.commands = commands;
        this.profilerAspect = profilerAspect;
    }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(GraphBench.class, args)));
    }

    @Override
    public void run(String... args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();

        // First stage - command
        options.addOption(Option.builder()
                .longOpt(ARG_COMMAND)
                .desc("Type of the test")
                .hasArg()
                .required()
                .build());

        // Process command line arguments
        try {
            CommandLine firstStageCmd = parser.parse(options, args, true);

            // Create command
            String commandName = firstStageCmd.getOptionValue(ARG_COMMAND);
            String commandNameLowerFirst = Character.toLowerCase(commandName.charAt(0)) + commandName.substring(1);
            AbstractCommand command = commands.get(commandNameLowerFirst);

            // Second stage - command parameters
            if (command.getOptions() != null) {
                command.getOptions().getOptions().forEach(options::addOption);
            }
            CommandLine secondStageCmd = parser.parse(options, args);

            // Put map of options
            Map<String, Option> optionsMap = Arrays.stream(secondStageCmd.getOptions())
                    .collect(Collectors.toMap(Option::getLongOpt, Function.identity()));

            // Start total group stats
            profilerAspect.startProfilingGroup("Total");

            // Main run
            command.run(optionsMap);

            // Print time statistics
            profilerAspect.printTimeStats();

        } catch (Exception e) {
            LOGGER.error(e.getMessage());

            if (LOGGER.isDebugEnabled()) {
                e.printStackTrace();
            }
        }
    }

    @Bean
    public ExitCodeGenerator exitCodeGenerator() {
        return () -> 0;
    }
}
