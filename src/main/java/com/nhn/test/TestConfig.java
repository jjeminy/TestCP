package com.nhn.test;

import com.nhn.test.metrics.MetricsTrackerFactory;
import com.nhn.test.util.PropertyElf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessControlException;
import java.sql.Connection;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;

import static com.nhn.test.util.UtilityElf.getNullIfEmpty;
import static com.nhn.test.util.UtilityElf.safeIsAssignableFrom;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestConfig implements TestConfigMXBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestConfig.class);

    private static final char[] ID_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final long CONNECTION_TIMEOUT = SECONDS.toMillis(30);
    private static final long VALIDATION_TIMEOUT = SECONDS.toMillis(5);
    private static final long IDLE_TIMEOUT = MINUTES.toMillis(10);
    private static final long MAX_LIFETIME = MINUTES.toMillis(30);
    private static final int DEFAULT_POOL_SIZE = 10;

    private static boolean unitTest = false;

    // changeable at runtime
    private volatile String catalog;
    private volatile long connectionTimeout;
    private volatile long validationTimeout;
    private volatile long idleTimeout;
    private volatile long leakDetectionThreshold;
    private volatile long maxLifetime;
    private volatile int maxPoolSize;
    private volatile int minIdle;
    private volatile String username;
    private volatile String password;

    // NOT changeable at runtime
    private long initializationFailTimeout;
    private String connectionInitSql;
    private String connectionTestQuery;
    private String dataSourceClassName;
    private String dataSourceJndiName;
    private String driverClassName;
    private String exceptionOverrideClassName;
    private String jdbcUrl;
    private String poolName;
    private String schema;
    private String transactionIsolationName;
    private boolean isAutoCommit;
    private boolean isReadOnly;
    private boolean isIsolateInternalQueries;
    private boolean isRegisterMbeans;
    private boolean isAllowPoolSuspension;
    private DataSource dataSource;
    private Properties dataSourceProperties;
    private ThreadFactory threadFactory;
    private ScheduledExecutorService scheduledExecutor;
    private MetricsTrackerFactory metricsTrackerFactory;
    private Object metricRegistry;
    private Object healthCheckRegistry;
    private Properties healthCheckProperties;

    private volatile boolean sealed;

    public TestConfig() {
        dataSourceProperties = new Properties();

        minIdle = -1;
        maxPoolSize = -1;
        maxLifetime = MAX_LIFETIME;
        connectionTimeout = CONNECTION_TIMEOUT;
        validationTimeout = VALIDATION_TIMEOUT;
        idleTimeout = IDLE_TIMEOUT;
        initializationFailTimeout = 1;
        isAutoCommit = true;

        String systemProp = System.getProperty("testcp.configurationFile");
        if (systemProp != null) {
            loadProperties(systemProp);
        }
    }

    public TestConfig(Properties properties) {
        this();
        PropertyElf.setTargetFromProperties(this, properties);
    }

    public TestConfig(String propertyFileName) {
        this();
        loadProperties(propertyFileName);
    }

    // ***********************************************************************
    //                       TestConfigMXBean methods
    // ***********************************************************************

    @Override
    public String getCatalog() {
        return catalog;
    }

    @Override
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    @Override
    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    @Override
    public void setConnectionTimeout(long connectionTimeoutMs) {
        if (connectionTimeoutMs == 0) {
            this.connectionTimeout = Integer.MAX_VALUE;
        }
        else if (connectionTimeoutMs < 250) {
            throw new IllegalArgumentException("connectionTimeout cannot be less than 250ms");
        }
        else {
            this.connectionTimeout = connectionTimeoutMs;
        }
    }

    @Override
    public long getIdleTimeout() {
        return idleTimeout;
    }

    @Override
    public void setIdleTimeout(long idleTimeoutMs) {
        if (idleTimeoutMs < 0) {
            throw new IllegalArgumentException("idleTimeout cannot be negative");
        }
        this.idleTimeout = idleTimeoutMs;
    }

    @Override
    public long getLeakDetectionThreshold() {
        return leakDetectionThreshold;
    }

    @Override
    public void setLeakDetectionThreshold(long leakDetectionThresholdMs) {
        this.leakDetectionThreshold = leakDetectionThresholdMs;
    }

    @Override
    public long getMaxLifetime() {
        return maxLifetime;
    }

    @Override
    public void setMaxLifetime(long maxLifetimeMs) {
        this.maxLifetime = maxLifetimeMs;
    }

    @Override
    public int getMaximumPoolSize() {
        return maxPoolSize;
    }

    @Override
    public void setMaximumPoolSize(int maxPoolSize) {
        if (maxPoolSize < 1) {
            throw new IllegalArgumentException("maxPoolSize cannot be less than 1");
        }
        this.maxPoolSize = maxPoolSize;
    }

    @Override
    public int getMinimumIdle() {
        return minIdle;
    }

    @Override
    public void setMinimumIdle(int minIdle) {
        if (minIdle < 0) {
            throw new IllegalArgumentException("minimumIdle cannot be negative");
        }
        this.minIdle = minIdle;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public long getValidationTimeout() {
        return validationTimeout;
    }

    @Override
    public void setValidationTimeout(long validationTimeoutMs) {
        if (validationTimeoutMs < 250) {
            throw new IllegalArgumentException("validationTimeout cannot be less than 250ms");
        }

        this.validationTimeout = validationTimeoutMs;
    }

    // ***********************************************************************
    //                     All other configuration methods
    // ***********************************************************************

    public String getConnectionTestQuery() {
        return connectionTestQuery;
    }

    public void setConnectionTestQuery(String connectionTestQuery) {
        checkIfSealed();
        this.connectionTestQuery = connectionTestQuery;
    }

    public String getConnectionInitSql() {
        return connectionInitSql;
    }

    public void setConnectionInitSql(String connectionInitSql) {
        checkIfSealed();
        this.connectionInitSql = connectionInitSql;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        checkIfSealed();
        this.dataSource = dataSource;
    }

    public String getDataSourceClassName() {
        return dataSourceClassName;
    }

    public void setDataSourceClassName(String className) {
        checkIfSealed();
        this.dataSourceClassName = className;
    }

    public void addDataSourceProperty(String propertyName, Object value) {
        checkIfSealed();
        dataSourceProperties.put(propertyName, value);
    }

    public String getDataSourceJNDI() {
        return this.dataSourceJndiName;
    }

    public void setDataSourceJNDI(String jndiDataSource) {
        checkIfSealed();
        this.dataSourceJndiName = jndiDataSource;
    }

    public Properties getDataSourceProperties() {
        return dataSourceProperties;
    }

    public void setDataSourceProperties(Properties dsProperties) {
        checkIfSealed();
        dataSourceProperties.putAll(dsProperties);
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        checkIfSealed();

        Class<?> driverClass = attemptFromContextLoader(driverClassName);
        try {
            if (driverClass == null) {
                driverClass = this.getClass().getClassLoader().loadClass(driverClassName);
                LOGGER.debug("Driver class {} found in the TestConfig class classloader {}", driverClassName, this.getClass().getClassLoader());
            }
        } catch (ClassNotFoundException e) {
            LOGGER.error("Failed to load driver class {} from TestConfig class classloader {}", driverClassName, this.getClass().getClassLoader());
        }

        if (driverClass == null) {
            throw new RuntimeException("Failed to load driver class " + driverClassName + " in either of TestConfig class loader or Thread context classloader");
        }

        try {
            driverClass.getConstructor().newInstance();
            this.driverClassName = driverClassName;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to instantiate class " + driverClassName, e);
        }
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        checkIfSealed();
        this.jdbcUrl = jdbcUrl;
    }

    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    public void setAutoCommit(boolean isAutoCommit) {
        checkIfSealed();
        this.isAutoCommit = isAutoCommit;
    }

    public boolean isAllowPoolSuspension() {
        return isAllowPoolSuspension;
    }

    public void setAllowPoolSuspension(boolean isAllowPoolSuspension) {
        checkIfSealed();
        this.isAllowPoolSuspension = isAllowPoolSuspension;
    }

    public long getInitializationFailTimeout() {
        return initializationFailTimeout;
    }

    public void setInitializationFailTimeout(long initializationFailTimeout) {
        checkIfSealed();
        this.initializationFailTimeout = initializationFailTimeout;
    }

    public boolean isIsolateInternalQueries() {
        return isIsolateInternalQueries;
    }

    public void setIsolateInternalQueries(boolean isolate) {
        checkIfSealed();
        this.isIsolateInternalQueries = isolate;
    }

//    public MetricsTrackerFactory getMetricsTrackerFactory() {
//        return metricsTrackerFactory;
//    }
//
//    public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
//        if (metricRegistry != null) {
//            throw new IllegalStateException("cannot use setMetricsTrackerFactory() and setMetricRegistry() together");
//        }
//
//        this.metricsTrackerFactory = metricsTrackerFactory;
//    }
//
//    public Object getMetricRegistry() {
//        return metricRegistry;
//    }
//
//    public void setMetricRegistry(Object metricRegistry) {
//        if (metricsTrackerFactory != null) {
//            throw new IllegalStateException("cannot use setMetricRegistry() and setMetricsTrackerFactory() together");
//        }
//
//        if (metricRegistry != null) {
//            metricRegistry = getObjectOrPerformJndiLookup(metricRegistry);
//
//            if (!safeIsAssignableFrom(metricRegistry, "com.codahale.metrics.MetricRegistry")
//                    && !(safeIsAssignableFrom(metricRegistry, "io.micrometer.core.instrument.MeterRegistry"))) {
//                throw new IllegalArgumentException("Class must be instance of com.codahale.metrics.MetricRegistry or io.micrometer.core.instrument.MeterRegistry");
//            }
//        }
//
//        this.metricRegistry = metricRegistry;
//    }
//
//    public Object getHealthCheckRegistry() {
//        return healthCheckRegistry;
//    }
//
//    public void setHealthCheckRegistry(Object healthCheckRegistry)
//    {
//        checkIfSealed();
//
//        if (healthCheckRegistry != null) {
//            healthCheckRegistry = getObjectOrPerformJndiLookup(healthCheckRegistry);
//
//            if (!(healthCheckRegistry instanceof HealthCheckRegistry)) {
//                throw new IllegalArgumentException("Class must be an instance of com.codahale.metrics.health.HealthCheckRegistry");
//            }
//        }
//
//        this.healthCheckRegistry = healthCheckRegistry;
//    }
//
//    public Properties getHealthCheckProperties() {
//        return healthCheckProperties;
//    }
//
//    public void setHealthCheckProperties(Properties healthCheckProperties) {
//        checkIfSealed();
//        this.healthCheckProperties.putAll(healthCheckProperties);
//    }
//
//    public void addHealthCheckProperty(String key, String value) {
//        checkIfSealed();
//        healthCheckProperties.setProperty(key, value);
//    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public void setReadOnly(boolean readOnly) {
        checkIfSealed();
        this.isReadOnly = readOnly;
    }

    public boolean isRegisterMbeans() {
        return isRegisterMbeans;
    }

    public void setRegisterMbeans(boolean register) {
        checkIfSealed();
        this.isRegisterMbeans = register;
    }

    @Override
    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        checkIfSealed();
        this.poolName = poolName;
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    public void setScheduledExecutor(ScheduledExecutorService executor) {
        checkIfSealed();
        this.scheduledExecutor = executor;
    }

    public String getTransactionIsolation() {
        return transactionIsolationName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        checkIfSealed();
        this.schema = schema;
    }

    public String getExceptionOverrideClassName() {
        return this.exceptionOverrideClassName;
    }

    public void setExceptionOverrideClassName(String exceptionOverrideClassName) {
        checkIfSealed();

        Class<?> overrideClass = attemptFromContextLoader(exceptionOverrideClassName);
        try {
            if (overrideClass == null) {
                overrideClass = this.getClass().getClassLoader().loadClass(exceptionOverrideClassName);
                LOGGER.debug("SQLExceptionOverride class {} found in the TestConfig class classloader {}", exceptionOverrideClassName, this.getClass().getClassLoader());
            }
        } catch (ClassNotFoundException e) {
            LOGGER.error("Failed to load SQLExceptionOverride class {} from TestConfig class classloader {}", exceptionOverrideClassName, this.getClass().getClassLoader());
        }

        if (overrideClass == null) {
            throw new RuntimeException("Failed to load SQLExceptionOverride class " + exceptionOverrideClassName + " in either of TestConfig class loader or Thread context classloader");
        }

        try {
            overrideClass.getConstructor().newInstance();
            this.exceptionOverrideClassName = exceptionOverrideClassName;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to instantiate class " + exceptionOverrideClassName, e);
        }
    }

    public void setTransactionIsolation(String isolationLevel) {
        checkIfSealed();
        this.transactionIsolationName = isolationLevel;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public void setThreadFactory(ThreadFactory threadFactory) {
        checkIfSealed();
        this.threadFactory = threadFactory;
    }

    void seal() {
        this.sealed = true;
    }

    public void copyStateTo(TestConfig other)
    {
        for (Field field : TestConfig.class.getDeclaredFields()) {
            if (!Modifier.isFinal(field.getModifiers())) {
                field.setAccessible(true);
                try {
                    field.set(other, field.get(this));
                }
                catch (Exception e) {
                    throw new RuntimeException("Failed to copy TestConfig state: " + e.getMessage(), e);
                }
            }
        }

        other.sealed = false;
    }

    // ***********************************************************************
    //                          Private methods
    // ***********************************************************************

    private Class<?> attemptFromContextLoader(final String driverClassName) {
        final ClassLoader threadContextClassLoader = Thread.currentThread().getContextClassLoader();
        if (threadContextClassLoader != null) {
            try {
                final Class<?> driverClass = threadContextClassLoader.loadClass(driverClassName);
                LOGGER.debug("Driver class {} found in Thread context class loader {}", driverClassName, threadContextClassLoader);
                return driverClass;
            } catch (ClassNotFoundException e) {
                LOGGER.debug("Driver class {} not found in Thread context class loader {}, trying classloader {}",
                        driverClassName, threadContextClassLoader, this.getClass().getClassLoader());
            }
        }

        return null;
    }

    public void validate() {
        if (poolName == null) {
            poolName = generatePoolName();
        }
        else if (isRegisterMbeans && poolName.contains(":")) {
            throw new IllegalArgumentException("poolName cannot contain ':' when used with JMX");
        }

        // treat empty property as null
        //noinspection NonAtomicOperationOnVolatileField
        catalog = getNullIfEmpty(catalog);
        connectionInitSql = getNullIfEmpty(connectionInitSql);
        connectionTestQuery = getNullIfEmpty(connectionTestQuery);
        transactionIsolationName = getNullIfEmpty(transactionIsolationName);
        dataSourceClassName = getNullIfEmpty(dataSourceClassName);
        dataSourceJndiName = getNullIfEmpty(dataSourceJndiName);
        driverClassName = getNullIfEmpty(driverClassName);
        jdbcUrl = getNullIfEmpty(jdbcUrl);

        // Check Data Source Options
        if (dataSource != null) {
            if (dataSourceClassName != null) {
                LOGGER.warn("{} - using dataSource and ignoring dataSourceClassName.", poolName);
            }
        }
        else if (dataSourceClassName != null) {
            if (driverClassName != null) {
                LOGGER.error("{} - cannot use driverClassName and dataSourceClassName together.", poolName);
                // NOTE: This exception text is referenced by a Spring Boot FailureAnalyzer, it should not be
                // changed without first notifying the Spring Boot developers.
                throw new IllegalStateException("cannot use driverClassName and dataSourceClassName together.");
            }
            else if (jdbcUrl != null) {
                LOGGER.warn("{} - using dataSourceClassName and ignoring jdbcUrl.", poolName);
            }
        }
        else if (jdbcUrl != null || dataSourceJndiName != null) {
            // ok
        }
        else if (driverClassName != null) {
            LOGGER.error("{} - jdbcUrl is required with driverClassName.", poolName);
            throw new IllegalArgumentException("jdbcUrl is required with driverClassName.");
        }
        else {
            LOGGER.error("{} - dataSource or dataSourceClassName or jdbcUrl is required.", poolName);
            throw new IllegalArgumentException("dataSource or dataSourceClassName or jdbcUrl is required.");
        }

        validateNumerics();

        if (LOGGER.isDebugEnabled() || unitTest) {
            logConfiguration();
        }
    }

    private void validateNumerics() {
        if (maxLifetime != 0 && maxLifetime < SECONDS.toMillis(30)) {
            LOGGER.warn("{} - maxLifetime is less than 30000ms, setting to default {}ms.", poolName, MAX_LIFETIME);
            maxLifetime = MAX_LIFETIME;
        }

        if (leakDetectionThreshold > 0 && !unitTest) {
            if (leakDetectionThreshold < SECONDS.toMillis(2) || (leakDetectionThreshold > maxLifetime && maxLifetime > 0)) {
                LOGGER.warn("{} - leakDetectionThreshold is less than 2000ms or more than maxLifetime, disabling it.", poolName);
                leakDetectionThreshold = 0;
            }
        }

        if (connectionTimeout < 250) {
            LOGGER.warn("{} - connectionTimeout is less than 250ms, setting to {}ms.", poolName, CONNECTION_TIMEOUT);
            connectionTimeout = CONNECTION_TIMEOUT;
        }

        if (validationTimeout < 250) {
            LOGGER.warn("{} - validationTimeout is less than 250ms, setting to {}ms.", poolName, VALIDATION_TIMEOUT);
            validationTimeout = VALIDATION_TIMEOUT;
        }

        if (maxPoolSize < 1) {
            maxPoolSize = DEFAULT_POOL_SIZE;
        }

        if (minIdle < 0 || minIdle > maxPoolSize) {
            minIdle = maxPoolSize;
        }

        if (idleTimeout + SECONDS.toMillis(1) > maxLifetime && maxLifetime > 0 && minIdle < maxPoolSize) {
            LOGGER.warn("{} - idleTimeout is close to or more than maxLifetime, disabling it.", poolName);
            idleTimeout = 0;
        }
        else if (idleTimeout != 0 && idleTimeout < SECONDS.toMillis(10) && minIdle < maxPoolSize) {
            LOGGER.warn("{} - idleTimeout is less than 10000ms, setting to default {}ms.", poolName, IDLE_TIMEOUT);
            idleTimeout = IDLE_TIMEOUT;
        }
        else  if (idleTimeout != IDLE_TIMEOUT && idleTimeout != 0 && minIdle == maxPoolSize) {
            LOGGER.warn("{} - idleTimeout has been set but has no effect because the pool is operating as a fixed size pool.", poolName);
        }
    }

    private void checkIfSealed() {
        if (sealed) {
            throw new IllegalStateException("The configuration of the pool is sealed once started. Use TestConfigMXBean for runtime changes.");
        }
    }

    private void logConfiguration() {
        LOGGER.debug("{} - configuration:", poolName);
        final Set<String> propertyNames = new TreeSet<>(PropertyElf.getPropertyNames(TestConfig.class));
        for (String prop : propertyNames) {
            try {
                Object value = PropertyElf.getProperty(prop, this);
                if ("dataSourceProperties".equals(prop)) {
                    Properties dsProps = PropertyElf.copyProperties(dataSourceProperties);
                    dsProps.setProperty("password", "<masked>");
                    value = dsProps;
                }

                if ("initializationFailTimeout".equals(prop) && initializationFailTimeout == Long.MAX_VALUE) {
                    value = "infinite";
                }
                else if ("transactionIsolation".equals(prop) && transactionIsolationName == null) {
                    value = "default";
                }
                else if (prop.matches("scheduledExecutorService|threadFactory") && value == null) {
                    value = "internal";
                }
                else if (prop.contains("jdbcUrl") && value instanceof String) {
                    value = ((String)value).replaceAll("([?&;]password=)[^&#;]*(.*)", "$1<masked>$2");
                }
                else if (prop.contains("password")) {
                    value = "<masked>";
                }
                else if (value instanceof String) {
                    value = "\"" + value + "\""; // quote to see lead/trailing spaces is any
                }
                else if (value == null) {
                    value = "none";
                }
                LOGGER.debug((prop + "................................................").substring(0, 32) + value);
            }
            catch (Exception e) {
                // continue
            }
        }
    }

    private void loadProperties(String propertyFileName) {
        final File propFile = new File(propertyFileName);
        try (final InputStream is = propFile.isFile() ? new FileInputStream(propFile) : this.getClass().getResourceAsStream(propertyFileName)) {
            if (is != null) {
                Properties props = new Properties();
                props.load(is);
                PropertyElf.setTargetFromProperties(this, props);
            }
            else {
                throw new IllegalArgumentException("Cannot find property file: " + propertyFileName);
            }
        }
        catch (IOException io) {
            throw new RuntimeException("Failed to read property file", io);
        }
    }

    private String generatePoolName() {
        final String prefix = "TestPool-";
        try {
            // Pool number is global to the VM to avoid overlapping pool numbers in classloader scoped environments
            synchronized (System.getProperties()) {
                final String next = String.valueOf(Integer.getInteger("com.nhn.test.pool_number", 0) + 1);
                System.setProperty("com.nhn.test.pool_number", next);
                return prefix + next;
            }
        } catch (AccessControlException e) {
            // The SecurityManager didn't allow us to read/write system properties
            // so just generate a random pool number instead
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            final StringBuilder buf = new StringBuilder(prefix);

            for (int i = 0; i < 4; i++) {
                buf.append(ID_CHARACTERS[random.nextInt(62)]);
            }

            LOGGER.info("assigned random pool name '{}' (security manager prevented access to system properties)", buf);

            return buf.toString();
        }
    }

    private Object getObjectOrPerformJndiLookup(Object object) {
        if (object instanceof String) {
            try {
                InitialContext initCtx = new InitialContext();
                return initCtx.lookup((String) object);
            }
            catch (NamingException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return object;
    }
}
