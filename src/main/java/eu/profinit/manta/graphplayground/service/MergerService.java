package eu.profinit.manta.graphplayground.service;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import eu.profinit.manta.graphplayground.model.manta.LicenseException;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.ItemTypes;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerType;
import eu.profinit.manta.graphplayground.model.manta.merger.MergingGroup;
import eu.profinit.manta.graphplayground.model.manta.merger.SpecificDataHolder;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.MergerOutput;
import eu.profinit.manta.graphplayground.model.manta.util.CsvHelper;
import eu.profinit.manta.graphplayground.repository.merger.manager.MergerManager;
import eu.profinit.manta.graphplayground.repository.merger.processor.FootprintCsvReader;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Primary
public class MergerService {

    /**
     * Merge files encoding.
     */
    protected static final String ENCODING = "utf-8";

    /**
     * Manager for merging.
     */
    private final MergerManager mergerManager;


    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MergerService.class);


    public MergerService(MergerManager mergerManager) {
        this.mergerManager = mergerManager;
    }

    protected void storeScripts(FootprintCsvReader footprintCsvReader) throws IOException, LicenseException, CsvValidationException {
        String[] itemParameters;
        while ((itemParameters = footprintCsvReader.readNext()) != null) {
            if (ArrayUtils.isEmpty(itemParameters)) {
                continue;
            }
            ItemTypes itemType = ItemTypes.parseString(itemParameters[0]);
            if (!ItemTypes.SOURCE_CODE.equals(itemType)) {
                break;
            }
            if (itemParameters.length != ItemTypes.SOURCE_CODE.getSize()) {
                continue;
            }
        }
        footprintCsvReader.closeFootprint();
    }

    /**
     * Provede zamergování grafu v inputstreamu do db.
     * @param inputStream vstupní stream obsahující linearizovaný graf pro zmergování
     * @param dataHolder speifická data pro konrkténí request předaná merger procesoru
     * @param revision revize číslo revize, do které se merguje
     * @return mapa pro view
     */
    public Map<String, Object> process(InputStream inputStream, SpecificDataHolder dataHolder,
                                       Double revision, MergerType mergerType) {
        Map<String, Object> resultMap = processMerge(inputStream, dataHolder, revision, mergerType);
        return resultMap;
    }

    /**
     * Common method which processes initializes database and store scripts.
     * @param inputStream Merger input stream.
     * @param dataHolder Additional merger data.
     * @param revision Merged revision.
     * @param mergerType Type of merging.
     * @return Map of merge result.
     */
    private Map<String, Object> processMerge(InputStream inputStream, SpecificDataHolder dataHolder,
                                               Double revision, MergerType mergerType) {
        Map<String, Object> resultMap = new HashMap<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(inputStream, ENCODING));
            CSVReader csvReader = CsvHelper.getCSVReader(reader);
            FootprintCsvReader footprintCsvReader = new FootprintCsvReader(csvReader);
            storeScripts(footprintCsvReader);
            resultMap = processMerge(footprintCsvReader, revision, mergerType);
        } catch (CsvValidationException | IOException e) {
            LOGGER.error("Error during read input stream.", e);
            resultMap.put("error", "Error during read input stream.");
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return resultMap;
    }

    /**
     * Processes inputstream and calls stored procedure.
     * @param footprintCsvReader Input stream reader.
     * @param revision Merged revision.
     * @return Map of merge result.
     * @throws IOException Problem with reading input stream.
     * @throws CsvValidationException Problem with reading CSV file.
     */
    private Map<String, Object> processMerge(FootprintCsvReader footprintCsvReader, Double revision,
                                                   MergerType mergerType)
            throws IOException, CsvValidationException {
        Map<String, Object> resultMap = new HashMap<>();
        List<List<List<String>>> mergedObjectsGroups = getMergeObjectsGroups(footprintCsvReader);

        MergerOutput mergerOutput = submitToMerge(mergedObjectsGroups, mergerType, revision);
        resultMap.put("processingReport", mergerOutput);
        return resultMap;
    }

    /**
     * Read whole input stream from CSV Reader.
     * Output list is initialized by 7 empty lists for each object type - list index
     * is determined by object's type.
     *
     * @param footprintCsvReader Footprint CSV Reader.
     * @return List of groups of merge objects.
     * @throws IOException Thrown in case of problem with reading.
     * @throws CsvValidationException Thrown in case of CSV validation errors.
     */
    private List<List<List<String>>> getMergeObjectsGroups(FootprintCsvReader footprintCsvReader) throws IOException, CsvValidationException {
        List<List<List<String>>> mergedObjectsGroups =
                Stream.generate(() -> (List<List<String>>) new ArrayList<List<String>>())
                        .limit(ItemTypes.values().length)
                        .collect(Collectors.toList());
        while (true) {
            String[] itemParameters = footprintCsvReader.readNext();
            if (itemParameters == null) break;
            DataflowObjectFormats.ItemTypes itemType = DataflowObjectFormats.ItemTypes.parseString(itemParameters[0]);
            int listIndex = itemType.getInputFilePosition();
            mergedObjectsGroups.get(listIndex).add(Arrays.asList(itemParameters));
            if (footprintCsvReader.isFootprintRead()) {
                if (DataflowObjectFormats.ItemTypes.SOURCE_CODE.equals(itemType)) {
                    throw new IllegalStateException("Unexpected source code position.");
                }
            }
        }
        return mergedObjectsGroups;
    }

    /**
     * Submits merge objects to manager for further processing.
     * @param mergedObjectsGroups List of groups of merge objects.
     * @param mergerType Type of merger.
     * @param revision Merged revision.
     * @return Merger output.
     */
    public MergerOutput submitToMerge(List<List<List<String>>> mergedObjectsGroups,
                                             MergerType mergerType, double revision) {
        MergingGroup mergingGroup = new MergingGroup(mergerType, revision);
        mergerManager.register(mergingGroup);
        MergerOutput mergerOutput = mergerManager.merge(mergedObjectsGroups);
        mergerManager.unregister();
        return mergerOutput;
    }
}