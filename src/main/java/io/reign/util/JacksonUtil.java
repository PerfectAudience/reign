/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package io.reign.util;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

/**
 * 
 * @author ypai
 * 
 */
public class JacksonUtil {
    /**
     * Reusable Jackson JSON mapper
     */
    private static ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();
    static {
        DEFAULT_OBJECT_MAPPER.getDeserializationConfig().without(
                DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
        DEFAULT_OBJECT_MAPPER.setSerializationInclusion(Inclusion.NON_NULL);

    }

    public static ObjectMapper getObjectMapper() {
        return DEFAULT_OBJECT_MAPPER;
    }
}
