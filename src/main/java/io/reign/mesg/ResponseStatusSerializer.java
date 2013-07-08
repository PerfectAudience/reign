package io.reign.mesg;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

public class ResponseStatusSerializer extends JsonSerializer<ResponseStatus> {

    @Override
    public void serialize(ResponseStatus value, JsonGenerator generator, SerializerProvider provider)
            throws IOException, JsonProcessingException {
        generator.writeNumber(value.code());
    }
}