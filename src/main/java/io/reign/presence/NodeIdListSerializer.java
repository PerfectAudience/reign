package io.reign.presence;

import java.io.IOException;
import java.util.List;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

/**
 * 
 * @author ypai
 * 
 */
public class NodeIdListSerializer extends JsonSerializer<List<String>> {
    @Override
    public void serialize(List<String> value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
            JsonProcessingException {
        jgen.writeStartArray();
        for (String rawJsonValue : value) {
            // Write value as raw data, since it's already JSON text
            jgen.writeRawValue(rawJsonValue);
        }
        jgen.writeEndArray();
    }
}