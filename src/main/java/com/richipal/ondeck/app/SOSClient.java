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
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

import com.richipal.ondeck.util.*;

/**
 * Created by rsingh on 4/19/14.
 */
public class SOSClient {
    public static void main(String[] args) throws Exception{

        //Read the command line options
        CommandLine cmdLine = parseArgs(args);
        String path = cmdLine.getOptionValue("file");
        String output = cmdLine.getOptionValue("output");

        CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);

        //Get the file name from path
        String fileName = StringUtils.substringAfterLast(path, "/");
        File csvFile = new File(path);
        if(!csvFile.exists()){
            throw new IOException("File:"+ path+" not found");
        }

        /*
         * This property state_mappings has the state mapping file information
         * which is mapping between the fields
         */
        List<String> stateMappings = Configurator.getList("state_mappings");
        List<String> orignalSchema = null;

        /**
         * We need to dynamically figure out which file it is and which schema to use
         * The assumption is the files are state names and the schemas are matched to the names in
         * the properties file. For example the california.csv incoming file will have a matching property california:camapping.json
         * where camapping.json has mappings of the fields with Master schema
         *
         */
        SOSParser parser=null;
        for(String mapping: stateMappings){
            String parts[] = mapping.split(":");
            /**
             * parts[0] will have the name of state and parts[1] will have the mapping file name
             */
            if(fileName.startsWith(parts[0])){
                /**
                 * Using Java Reflection dynamically invoke the constructor on SOSParser and set the mapping file
                 */
                Class<?> clazz = SOSParser.class;
                Constructor<?> constructor = clazz.getConstructor(String.class);
                parser = (SOSParser) constructor.newInstance(parts[1]);

                //This is schema for csv file header, each state will have its own schema defined in the properties file.
                try{
                    orignalSchema = Configurator.getList(parts[0]+"_schema");
                }catch(Exception e){
                    throw new RuntimeException("Original Schema not found in the properties file, expecting a property name :"+parts[0]+"_schema"
                    +" in the mappings.properties file with the value of original schema.");
                }

                break;//once we identified the mappings break out of the for loop
            }

        }

        //If schema not found throw exception
        if(orignalSchema==null||orignalSchema.size()==0){
            throw new RuntimeException("Schema definition not found in mappings.properties file, please add it example:- california:camappings.json");
        }

        /**
         * If mapping files are found then go ahead and read through the csv file
         */
        MappingIterator<Object[]> it = mapper.reader(Object[].class).readValues(csvFile);
        List<String> fields = new ArrayList<String>();
        File outFile =new File(output);
        while (it.hasNext()) {
            /**
             * The first line is header line, which will help in comparing the existing schema
             * If field names change or new fields are added or fields are taken off
             * we throw Exception
             */
            if(it.getCurrentLocation().getLineNr()==1){
                Object[] row = it.next();
                for(Object field: row){
                    fields.add((String) field);
                }

                // We do union and intersection of lists and compare for schema mismatch
                List<String> union = ListUtils.union(orignalSchema,fields);
                List<String> intersection = ListUtils.intersection(orignalSchema,fields);
                union.removeAll(intersection);

                //If Schema not same throw the Exception else continue
                if(union.size()>0) {
                    throw new Exception("There are Schemas differences the field names don't match:"+union.toString());
                }

                //Check the size of original schema and the new schema If they are different then throw exception
                if(!((intersection.size()==orignalSchema.size())&&(intersection.size()==fields.size()))) {
                    throw new Exception("Schemas not same please check the incoming csv file schema");
                }

                continue;//read next line
            }
            Object[] row = it.next();

            /**
             * If no differences in schema then,create a JSONOject from line read.
             * This is mapped to the fields read from first line in the csv.
             */
            JSONObject incomingJSON = SchemaUtils.mapField(fields, row);
//            System.out.println("Incoming:>>"+incomingJSON.toJSONString());

            /**
             * Use the JSON Object created from the csv and create a new MasterSchema object
             * this uses schema mapping file, If the file is not found it will throw exception.
             */
            MasterSchema masterSchema = SchemaUtils.mapToMasterSchema(incomingJSON,parser.getSchemaMapping());

            //Here we add some additional fields that might help.
            AvroUtils.setDatumAttribute(masterSchema,"timestamp", ""+DateTimeUtils.currentTimeMillis());
            AvroUtils.setDatumAttribute(masterSchema,"yyyyMMddHHmmssSSS", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss:SSS").print(new DateTime()));
            AvroUtils.setDatumAttribute(masterSchema,"yyyyMMddHH", DateTimeFormat.forPattern("yyyyMMddHH").print(new DateTime()));
            AvroUtils.setDatumAttribute(masterSchema,"yyyyMM", DateTimeFormat.forPattern("yyyyMM").print(new DateTime()));
            AvroUtils.setDatumAttribute(masterSchema,"fileName", fileName);
              System.out.println("Master:-----"+masterSchema.toString());
//            FileOutputStream outputStream = new FileOutputStream(new File(output));
            FileUtils.write(outFile, masterSchema.toString() + "\n", true);

        }

    }



    /*
     * Reads the command line options
     */
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
