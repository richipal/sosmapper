package com.richipal.ondeck.app;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.richipal.ondeck.parser.SOSParser;
import com.richipal.ondeck.schema.avro.MasterSchema;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.json.simple.JSONObject;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.*;

import com.richipal.ondeck.util.*;

/**
 * Created by rsingh on 4/19/14.
 */
public class SOSClient {
    public static void main(String[] args) throws Exception{

        CommandLine cmdLine = parseArgs(args);
        String path = cmdLine.getOptionValue("file");
        String output = cmdLine.getOptionValue("output");

        CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);

        //Get the file name from path
        String fileName = StringUtils.substringAfterLast(path, "/");
        File csvFile = new File(path);

        List<String> stateMappings = Configurator.getList("state_mappings");
        List<String> orignalSchema = null;

        /**
         * We need to dynamically figure out which file it is and which schema to use
         * The assumption is the files are state names and the schemas are matched to the names in
         * the properties file. For example the california.csv has a matching property california:fully_qualified_parser_name
         *
         */
        SOSParser parser=null;
        for(String mapping: stateMappings){
            String parts[] = mapping.split(":");
            if(fileName.startsWith(parts[0])){
                /**
                 * Instantiate the parser class dynamically based on file type
                 *parts[0] will contain the stateName and parts[1] will have parser Class name
                 */
                Class<?> clazz = Class.forName("com.richipal.ondeck.parser.SOSParser");
                Constructor<?> constructor = clazz.getConstructor(String.class);
                parser = (SOSParser) constructor.newInstance(parts[1]);

                //This is schema for csv file header, each state will have its own schema defined in the properties file.
                orignalSchema = Configurator.getList(parts[0]+"_schema");

                break;//once we identified the parser break out of the for loop
            }
        }

        /**
         * Read through the csv file
         */
        MappingIterator<Object[]> it = mapper.reader(Object[].class).readValues(csvFile);
        List<String> fields = new ArrayList<String>();
        File outFile =new File(output);
        while (it.hasNext()) {
            //The first line is header line
            if(it.getCurrentLocation().getLineNr()==1){
                Object[] row = it.next();
                for(Object field: row){
                    fields.add((String) field);
                }

                List<String> union = ListUtils.union(orignalSchema,fields);
                List<String> intersection = ListUtils.intersection(orignalSchema,fields);
                union.removeAll(intersection);

                //If Schema not same throw the Exception else continue
                if(union.size()>0) {
                    throw new Exception("Schemas differences:"+union.toString());
                }

                if(!((intersection.size()==orignalSchema.size())&&(intersection.size()==fields.size()))) {
                    throw new Exception("Schemas not same please check the incoming schema");
                }

                continue;//read next line
            }
            Object[] row = it.next();
            JSONObject incomingJSON = SchemaUtils.mapField(fields, row);
            System.out.println("Incoming:>>"+incomingJSON.toJSONString());
            MasterSchema masterSchema = SchemaUtils.mapToMasterSchema(incomingJSON,parser.getSchemaMapping());

            AvroUtils.setDatumAttribute(masterSchema,"timestamp", ""+DateTimeUtils.currentTimeMillis());
            AvroUtils.setDatumAttribute(masterSchema,"yyyyMMddHHmmssSSS", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss:SSS").print(new DateTime()));
            AvroUtils.setDatumAttribute(masterSchema,"yyyyMMddHH", DateTimeFormat.forPattern("yyyyMMddHH").print(new DateTime()));
            AvroUtils.setDatumAttribute(masterSchema,"fileName", fileName);
              System.out.println("Master:-----"+masterSchema.toString());
//            FileOutputStream outputStream = new FileOutputStream(new File(output));
            FileUtils.write(outFile, masterSchema.toString() + "\n", true);

        }

    }



    private static CommandLine parseArgs(String[] args) throws org.apache.commons.cli.ParseException {

        Options options = new Options();

        Option o;

        o = new Option("f", "file", true, "CSV File to read");
        o.setArgName("file");
        o.setRequired(false);
        options.addOption(o);

        o = new Option("o", "output", true, "Output file");
        o.setArgName("output");
        o.setRequired(false);
        options.addOption(o);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
            if (!cmd.hasOption("input") && !cmd.hasOption("output")) {
                throw new RuntimeException("Invalid usage");
            }
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();

            formatter.printHelp("SOSClient ", options, true);
            System.exit(1);
        }

        return cmd;
    }
}
