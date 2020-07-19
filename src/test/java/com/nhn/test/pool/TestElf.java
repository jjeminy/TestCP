package com.nhn.test.pool;

import com.nhn.test.TestConfig;
import com.nhn.test.TestDataSource;
import com.nhn.test.util.ConcurrentBag;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.SyslogLayout;
import org.apache.logging.slf4j.Log4jLogger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.sql.Connection;

/**
 * Utility methods for testing.
 *
 * @author Brett Wooldridge
 */
public final class TestElf
{
    private TestElf() {
        // default constructor
    }

    public static boolean isJava11() {
        return System.getProperty("java.version").startsWith("11");
    }

    public static TestPool getPool(final TestDataSource ds)
    {
        try {
            Field field = ds.getClass().getDeclaredField("pool");
            field.setAccessible(true);
            return (TestPool) field.get(ds);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static ConcurrentBag<?> getConcurrentBag(final TestDataSource ds)
    {
        try {
            Field field = TestPool.class.getDeclaredField("connectionBag");
            field.setAccessible(true);
            return (ConcurrentBag<?>) field.get(getPool(ds));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TestConfig getUnsealedConfig(final TestDataSource ds)
    {
        try {
            TestPool pool = getPool(ds);
            Field configField = PoolBase.class.getDeclaredField("config");
            configField.setAccessible(true);
            TestConfig config = (TestConfig) configField.get(pool);

            Field field = TestConfig.class.getDeclaredField("sealed");
            field.setAccessible(true);
            field.setBoolean(config, false);
            return config;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static boolean getConnectionCommitDirtyState(final Connection connection)
    {
        try {
            Field field = ProxyConnection.class.getDeclaredField("isCommitStateDirty");
            field.setAccessible(true);
            return field.getBoolean(connection);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void setConfigUnitTest(final boolean unitTest)
    {
        try {
            Field field = TestConfig.class.getDeclaredField("unitTest");
            field.setAccessible(true);
            field.setBoolean(null, unitTest);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void setSlf4jTargetStream(final Class<?> clazz, final PrintStream stream)
    {
        try {
            Log4jLogger log4Jlogger = (Log4jLogger) LoggerFactory.getLogger(clazz);

            Field field = clazz.getClassLoader().loadClass("org.apache.logging.slf4j.Log4jLogger").getDeclaredField("logger");
            field.setAccessible(true);

            Logger logger = (Logger) field.get(log4Jlogger);
            if (logger.getAppenders().containsKey("string")) {
                Appender appender = logger.getAppenders().get("string");
                logger.removeAppender(appender);
            }

            logger.addAppender(new StringAppender("string", stream));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void setSlf4jLogLevel(final Class<?> clazz, final Level logLevel)
    {
        try {
            Log4jLogger log4Jlogger = (Log4jLogger) LoggerFactory.getLogger(clazz);

            Field field = clazz.getClassLoader().loadClass("org.apache.logging.slf4j.Log4jLogger").getDeclaredField("logger");
            field.setAccessible(true);

            Logger logger = (Logger) field.get(log4Jlogger);
            logger.setLevel(logLevel);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TestConfig newTestConfig()
    {
        final StackTraceElement callerStackTrace = Thread.currentThread().getStackTrace()[2];

        String poolName = callerStackTrace.getMethodName();
        if ("setup".equals(poolName)) {
            poolName = callerStackTrace.getClassName();
        }

        final TestConfig config = new TestConfig();
        config.setPoolName(poolName);
        return config;
    }

    static TestDataSource newTestDataSource()
    {
        final StackTraceElement callerStackTrace = Thread.currentThread().getStackTrace()[2];

        String poolName = callerStackTrace.getMethodName();
        if ("setup".equals(poolName)) {
            poolName = callerStackTrace.getClassName();
        }

        final TestDataSource ds = new TestDataSource();
        ds.setPoolName(poolName);
        return ds;
    }

    private static class StringAppender extends AbstractAppender
    {
        private PrintStream stream;

        StringAppender(final String name, final PrintStream stream)
        {
            super(name, null, SyslogLayout.newBuilder().build());
            this.stream = stream;
        }

        @Override
        public void append(final LogEvent event)
        {
            stream.println(event.getMessage().getFormattedMessage());
        }
    }

    public static class FauxWebClassLoader extends ClassLoader
    {
        static final byte[] classBytes = new byte[16_000];

        @Override
        public Class<?> loadClass(final String name) throws ClassNotFoundException
        {
            if (name.startsWith("java") || name.startsWith("org")) {
                return super.loadClass(name, true);
            }

            final String resourceName = "/" + name.replace('.', '/') + ".class";
            final URL resource = this.getClass().getResource(resourceName);
            try (DataInputStream is = new DataInputStream(resource.openStream())) {
                int read = 0;
                while (read < classBytes.length) {
                    final int rc = is.read(classBytes, read, classBytes.length - read);
                    if (rc == -1) {
                        break;
                    }
                    read += rc;
                }

                return defineClass(name, classBytes, 0, read);
            }
            catch (IOException e) {
                throw new ClassNotFoundException(name);
            }
        }
    }
}
