package com.nhn.test.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UtilityElfTest {
    @Test
    public void shouldReturnValidTransactionIsolationLevel()
    {
        //Act
        int expectedLevel = UtilityElf.getTransactionIsolation("TRANSACTION_SQL_SERVER_SNAPSHOT_ISOLATION_LEVEL");

        //Assert
        assertEquals(expectedLevel, 4096);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenInvalidTransactionNameGiven()
    {
        //Act
        UtilityElf.getTransactionIsolation("INVALID_TRANSACTION");
    }

    @Test
    public void shouldReturnTransationIsolationLevelFromInteger()
    {
        int expectedLevel = UtilityElf.getTransactionIsolation("4096");
        assertEquals(expectedLevel, 4096);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenInvalidTransactionIntegerGiven()
    {
        //Act
        UtilityElf.getTransactionIsolation("9999");
    }
}
