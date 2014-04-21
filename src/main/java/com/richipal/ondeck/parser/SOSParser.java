package com.richipal.ondeck.parser;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by rsingh on 4/19/14.
 */
public class SOSParser {

    private final String PATH = "src/main/resources/mappings/";
    private String mappingFile;

    public SOSParser(String mappingFile) {
        this.mappingFile = mappingFile;
    }

    public JSONObject getSchemaMapping() throws IOException{
        if(getMappingFile()==null){
            throw new IOException("Mapping File not found, please verify mapping file is provided");
        }
        JSONParser parser=new JSONParser();
        JSONObject jsonObject = null;
        try {
            jsonObject = (JSONObject) parser.parse(new FileReader(PATH+getMappingFile()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (org.json.simple.parser.ParseException e) {
            e.printStackTrace();
        }
        return jsonObject;

    }

    public String getMappingFile() {
        return mappingFile;
    }

    public void setMappingFile(String mappingFile) {
        this.mappingFile = mappingFile;
    }
}
