package ahbun.lib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class FixedSizePriorityQueue<T> {
    private static Logger logger = LoggerFactory.getLogger(FixedSizePriorityQueue.class);
    private TreeSet<T> treeSet;
    private int maxSize;
    private int currentSize;

    public FixedSizePriorityQueue(Comparator<T> comparator, int maxSize) {
        treeSet = new TreeSet<>(comparator);
        this.maxSize = maxSize;
        currentSize = 0;
    }

    public FixedSizePriorityQueue<T> add(T element) {
        treeSet.add(element);
        logger.debug("add called ");
        if (++currentSize > maxSize) {
            logger.debug("q size exceeded. Remove last");
            treeSet.pollLast();
        }
        logger.debug("add called current size: " + currentSize );
        return this;
    }

    public FixedSizePriorityQueue<T> remove(T element) {
        if (treeSet.contains(element)) {
            treeSet.remove(element);
            currentSize--;
            logger.debug("remove called current size: " + currentSize );
        }

        return this;
    }

    public int currentSize() {
        return currentSize;
    }

    public int maxSize() { return maxSize; }

    public Iterator<T> iterator() {
        return treeSet.iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        logger.debug("--------------- TO String Called - current size " + currentSize);
        if (currentSize > 0) {
            Iterator<T> it = iterator();
            while (it.hasNext()) {
                sb.append(it.next().toString());
                sb.append(", ");
            }

            String value = sb.toString();
            return value.substring(0, value.length() -2);
        } else {
            return "empty set";
        }
    }
}
