package com.nhn.test.util;

import com.nhn.test.mocks.TestObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

public class PropertyElfTest {
    @Test
    public void setTargetFromProperties() throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty("string", "aString");
        properties.setProperty("testObject", "com.nhn.test.mocks.TestObject");
        TestObject testObject = new TestObject();
        PropertyElf.setTargetFromProperties(testObject, properties);
        assertEquals("aString", testObject.getString());
        assertEquals(com.nhn.test.mocks.TestObject.class, testObject.getTestObject().getClass());
        assertNotSame(testObject, testObject.getTestObject());
    }

    @Test
    public void setTargetFromPropertiesNotAClass() throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty("string", "aString");
        properties.setProperty("testObject", "it is not a class");
        TestObject testObject = new TestObject();
        try {
            PropertyElf.setTargetFromProperties(testObject, properties);
            fail("Could never come here");
        }
        catch (RuntimeException e) {
            assertEquals("argument type mismatch", e.getCause().getMessage());
        }
    }
}
