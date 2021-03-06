package com.nhn.test.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import com.nhn.test.mocks.StubStatement;

public class TestFastList {
    @Test
    public void testAddRemove()
    {
        ArrayList<Statement> verifyList = new ArrayList<>();

        FastList<Statement> list = new FastList<>(Statement.class);
        for (int i = 0; i < 32; i++)
        {
            StubStatement statement = new StubStatement(null);
            list.add(statement);
            verifyList.add(statement);
        }

        for (int i = 0; i < 32; i++)
        {
            assertNotNull("Element " + i + " was null but should be " + verifyList.get(i), list.get(0));
            int size = list.size();
            list.remove(verifyList.get(i));
            assertSame(size - 1, list.size());
        }
    }

    @Test
    public void testAddRemoveTail()
    {
        ArrayList<Statement> verifyList = new ArrayList<>();

        FastList<Statement> list = new FastList<>(Statement.class);
        for (int i = 0; i < 32; i++)
        {
            StubStatement statement = new StubStatement(null);
            list.add(statement);
            verifyList.add(statement);
        }

        for (int i = 31; i >= 0; i--)
        {
            assertNotNull("Element " + i, list.get(i));
            int size = list.size();
            list.remove(verifyList.get(i));
            assertSame(size - 1, list.size());
        }
    }

    @Test
    public void testOverflow()
    {
        ArrayList<Statement> verifyList = new ArrayList<>();

        FastList<Statement> list = new FastList<>(Statement.class);
        for (int i = 0; i < 100; i++)
        {
            StubStatement statement = new StubStatement(null);
            list.add(statement);
            verifyList.add(statement);
        }

        for (int i = 0; i < 100; i++)
        {
            assertNotNull("Element " + i, list.get(i));
            assertSame(verifyList.get(i), list.get(i));
        }
    }

    @Test
    public void testIterator()
    {
        FastList<Statement> list = new FastList<>(Statement.class);
        for (int i = 0; i < 100; i++)
        {
            StubStatement statement = new StubStatement(null);
            list.add(statement);
        }

        Iterator<Statement> iter = list.iterator();
        for (int i = 0;  i < list.size(); i++) {
            assertSame(list.get(i), iter.next());
        }
    }

    @Test
    public void testClear()
    {
        FastList<Statement> list = new FastList<>(Statement.class);
        for (int i = 0; i < 100; i++)
        {
            StubStatement statement = new StubStatement(null);
            list.add(statement);
        }

        assertNotEquals(0, list.size());
        list.clear();
        assertEquals(0, list.size());
        // also check that all elements are now null
        for (int i = 0; i < 100; i++) {
            assertEquals(null, list.get(i));
        }
    }

    @Test
    public void testRemoveLast()
    {
        FastList<Statement> list = new FastList<>(Statement.class);

        Statement last = null;
        for (int i = 0; i < 100; i++)
        {
            StubStatement statement = new StubStatement(null);
            list.add(statement);
            last = statement;
        }

        assertEquals(last, list.removeLast());
        assertEquals(99, list.size());
    }

    @Test
    public void testPolyMorphism1()
    {
        class Foo implements Base2 {

        }

        class Bar extends Foo {

        }

        FastList<Base> list = new FastList<>(Base.class, 2);
        list.add(new Foo());
        list.add(new Foo());
        list.add(new Bar());
    }

    interface Base
    {

    }

    interface Base2 extends Base
    {

    }
}
