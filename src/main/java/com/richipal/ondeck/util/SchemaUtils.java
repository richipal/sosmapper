package com.richipal.ondeck.util;

import com.richipal.ondeck.schema.avro.MasterSchema;
import net.vidageek.mirror.dsl.Mirror;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Created by rsingh on 4/19/14.
 */
public class SchemaUtils {

    public static JSONObject mapField(List<String> fields, Object[] row){
        JSONObject result = new JSONObject();
        int i =0;
        for(String field:fields){
            result.put(field,row[i]);
            i++;
        }
        return result;
    }

    public static MasterSchema mapToMasterSchema(JSONObject incoming,JSONObject mappings){
        JSONObject result = new JSONObject();
        Set<String> keys = incoming.keySet();
        MasterSchema masterSchema = new MasterSchema();

        Mirror mirror = new Mirror();

        for( String key: keys ){
            //Put only the keys which we have a mapping for
            try{
                mirror.on(masterSchema).set().field((String) mappings.get(key)).withValue(incoming.get(key));
            }catch(Exception e){
                //If field not found ignore, default value null for all mismatched fields.
            }
        }
        return masterSchema;
    }

    public static JSONObject mapToMasterSchema2(JSONObject incoming, JSONObject mapping){
        JSONObject result = new JSONObject();
        Set<String> keys = incoming.keySet();
        for( String key: keys ){
            //Put only the keys which we have a mapping for
            if(mapping.get(key)!=null)
                result.put(mapping.get(key), incoming.get(key));
        }
        return result;
    }

    public static void setDatumAttribute(MasterSchema datum, String fieldName, Object val) throws IOException {
        try {
            AvroUtils.setDatumAttribute(datum, fieldName, val);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException();
        }
    }
}
