package io.reign.data;

import io.reign.AbstractDataSerializerTest;
import io.reign.DataSerializer;

import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.junit.Assert;
import org.junit.Before;

/**
 * Service discovery service.
 * 
 * @author francoislagier
 * @author daphnehsu
 * 
 */
public class JsonDataSerializerTest extends AbstractDataSerializerTest {

    private DataSerializer<SmallObject> serializer;
    private Random random;

    @Before
    public void setUp() throws Exception {
        serializer = new JsonDataSerializer<SmallObject>(SmallObject.class);
        random = new Random();
    }

    // @Test
    public void testBasic() {

        for (int i = 0; i < 10; i++) {
            SmallObject originalSmallObject = createRandomSmallObject();
            System.out.println("originalSmallObject:" + originalSmallObject);
            byte[] bytes = serializer.serialize(originalSmallObject);
            SmallObject reconstructedSmallObject = serializer.deserialize(bytes);
            System.out.println("reconstructedSmallObject:" + reconstructedSmallObject);

            Assert.assertTrue(reconstructedSmallObject.equals(originalSmallObject));
        }

    }

    private SmallObject createRandomSmallObject() {
        String uuid = UUID.randomUUID().toString();
        long longValue = random.nextLong();
        SmallEnum smallEnum = SmallEnum.values()[random.nextInt(SmallEnum.values().length)];
        return new SmallObject(uuid, longValue, smallEnum);
    }

    public static class SmallObject {
        String testString;
        Long testLong;
        SmallEnum smallEnum;

        @JsonCreator
        public SmallObject(@JsonProperty("testString") String testString, @JsonProperty("testLong") Long testLong,
                @JsonProperty("smallEnum") SmallEnum smallEnum) {
            this.testString = testString;
            this.testLong = testLong;
            this.smallEnum = smallEnum;
        }

        public String getTestString() {
            return testString;
        }

        public Long getTestLong() {
            return testLong;
        }

        public SmallEnum getSmallEnum() {
            return smallEnum;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((smallEnum == null) ? 0 : smallEnum.hashCode());
            result = prime * result + ((testLong == null) ? 0 : testLong.hashCode());
            result = prime * result + ((testString == null) ? 0 : testString.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SmallObject other = (SmallObject) obj;
            if (smallEnum != other.smallEnum)
                return false;
            if (testLong == null) {
                if (other.testLong != null)
                    return false;
            } else if (!testLong.equals(other.testLong))
                return false;
            if (testString == null) {
                if (other.testString != null)
                    return false;
            } else if (!testString.equals(other.testString))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.DEFAULT_STYLE);
        }

    }

    public static enum SmallEnum {
        A, B, C;
    }

}
