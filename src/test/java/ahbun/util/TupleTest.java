package ahbun.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TupleTest {
    private Tuple<Integer, String> tuple;

    @Before
    public void setup() {
        tuple = new Tuple<>(1, "A");
    }

    @Test
    public void getX() {
        Assert.assertTrue(1 == tuple.getX());
    }

    @Test
    public void getY() {

        Assert.assertTrue("A".equals(tuple.getY()));
    }

    @Test
    public void testEquals() {
        Tuple b = new Tuple(1, "A");
        Assert.assertTrue(b.equals(tuple));
        Tuple c = new Tuple(2, "A");
        Assert.assertFalse(c.equals(tuple));
        Tuple d = new Tuple(1, "B");
        Assert.assertFalse(d.equals(tuple));
    }

    @Test
    public void testToString() {
        Assert.assertTrue("Tuple{x=1, y=A}".equals(tuple.toString()));
    }
}