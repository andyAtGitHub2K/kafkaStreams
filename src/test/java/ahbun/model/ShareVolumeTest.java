package ahbun.model;

import ahbun.lib.FPQTypeAdapter;
import ahbun.lib.FixedSizePriorityQueue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class ShareVolumeTest {
    private ShareVolume.ShareVolumeBuilder builder;
    private ShareVolume shareVolumeA;
    private ShareVolume shareVolumeB;


    @Before
    public void setup() {
        builder = getBuilderA();
        shareVolumeA = builder.build();

        builder = getBuilderB();
        shareVolumeB = builder.build();
    }

    private ShareVolume.ShareVolumeBuilder getBuilderA() {
        builder = ShareVolume.builder();
        builder.industry("food")
                .symbol("abc")
                .volume(1000);
        return builder;
    }

    private ShareVolume.ShareVolumeBuilder getBuilderB() {
        builder = ShareVolume.builder();
        builder.industry("food")
                .symbol("abc")
                .volume(-500);
        return builder;
    }

    @Test
    public void builder() {
        ShareVolume v2 = getBuilderA().build();
        Assert.assertEquals(shareVolumeA, v2);
        Assert.assertTrue("food".equals(v2.getIndustry()));
        Assert.assertTrue("abc".equals(v2.getSymbol()));
        Assert.assertTrue(1000== v2.getVolume());
    }


    @Test
    public void sum() {
        Assert.assertTrue(shareVolumeA.getVolume() == 1000);
        Assert.assertTrue(shareVolumeB.getVolume() == -500);
        ShareVolume shareVolume = ShareVolume.sum(shareVolumeA, shareVolumeB);
        Assert.assertTrue(500 == shareVolume.getVolume());
    }

    @Test
    public void testPriorityQ() {
        Set<String> treeSet = new TreeSet<>();
        treeSet.add("C");
        treeSet.add("D");
        treeSet.add("A");
        Iterator<String> it = treeSet.iterator();
        while(it.hasNext()) {
            System.out.println(it.next());
        }
    }

    @Test
    public void testPriorityQWithComparator() {
        Comparator<String> comparator = (s1, s2) -> s2.compareTo(s1);
        Set<String> treeSet = new TreeSet<>( comparator);
        treeSet.add("C");
        treeSet.add("D");
        treeSet.add("A");
        Iterator<String> it = treeSet.iterator();
        while(it.hasNext()) {
            System.out.println(it.next());
        }
    }

    @Test
    public void testAddRemovePQ() {
        Comparator<String> comparator = (s1, s2) -> s2.compareTo(s1);
        FixedSizePriorityQueue<String> treeSet = new FixedSizePriorityQueue<>( comparator, 3);
        String[] data = {"X", "G", "R", "B", "K"};
        for (String a: data) {
            treeSet.add(a);
        }

        Assert.assertTrue(5  == treeSet.currentSize());
        System.out.println(treeSet.toString());
        Assert.assertTrue("X, R, K".equals(treeSet.toString()));
    }

    @Test
    public void testPQAdapter() {
        ShareVolume v1 = getBuilderA().build();
        ShareVolume v2 = getBuilderB().build();

        Comparator<ShareVolume> comparator = (s1, s2) -> s2.getVolume() - s1.getVolume();
        FixedSizePriorityQueue<ShareVolume> treeSet = new FixedSizePriorityQueue<>( comparator, 3);
            treeSet.add(v1);
        treeSet.add(v2);
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(FixedSizePriorityQueue.class,
                new FPQTypeAdapter().nullSafe());
        Gson gson = builder.create();

        String treeJson = gson.toJson(treeSet);
        System.out.println(treeJson);

        FixedSizePriorityQueue<ShareVolume> fpq = gson.fromJson(treeJson, FixedSizePriorityQueue.class);
        Assert.assertTrue(treeSet.maxSize() == fpq.maxSize());
        Assert.assertTrue(treeSet.currentSize() == fpq.currentSize());
        Assert.assertTrue(treeSet.currentSize() == fpq.maxSize() - 1);
        System.out.println(fpq);
        System.out.println(gson.toJson(fpq));
    }

    @Test
    public void testSVSerde() {
        Gson gson = new Gson();
        ShareVolume v1 = getBuilderA().build();
        String j = gson.toJson(v1, ShareVolume.class);
        System.out.println(j);

        String k = "{\"industry\":\"food\",\"symbol\":\"abc\",\"volume\":1000}";
        ShareVolume vj = gson.fromJson(k, ShareVolume.class);
        System.out.println(vj.toString());
        Assert.assertTrue(v1.equals(vj));
    }
}