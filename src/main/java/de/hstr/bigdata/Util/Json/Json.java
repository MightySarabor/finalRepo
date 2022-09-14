package de.hstr.bigdata.Util.Json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;

public class Json {
    private static ObjectMapper objectMapper = getDefaultObjectMapper();

    private static ObjectMapper getDefaultObjectMapper(){
        ObjectMapper defaultObjectMapper = new ObjectMapper();
        defaultObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return defaultObjectMapper;

    }
    //JSON String to JSON Node
    public static JsonNode parse(String src) throws JsonProcessingException {
        return objectMapper.readTree(src);
    }
    //JSON Node to POJO
    public static <A> A fromJson(JsonNode node, Class<A> claZZ) throws JsonProcessingException {
        return objectMapper.treeToValue(node, claZZ);
    }

    public static JsonNode toJson(Object a){
        return objectMapper.valueToTree(a);
    }

    public static String stringify(JsonNode node) throws JsonProcessingException {
        return generateString(node, false);
    }

    public static String prettyPrint(JsonNode node) throws JsonProcessingException {
        return generateString(node, true);
    }

    private static String generateString(JsonNode node, boolean pretty) throws JsonProcessingException {
        ObjectWriter objectWriter = objectMapper.writer();
        if(pretty){
            objectWriter = objectWriter.with(SerializationFeature.INDENT_OUTPUT);
        }
        return objectWriter.writeValueAsString(node);
    }



}
