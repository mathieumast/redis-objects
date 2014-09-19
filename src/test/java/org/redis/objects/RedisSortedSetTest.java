package org.redis.objects;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Mathieu MAST
 */
public class RedisSortedSetTest {

    public static class ScoreableString implements Scoreable {

        private String str;

        private double weight;

        public ScoreableString() {
        }

        public ScoreableString(String str, double weight) {
            this.str = str;
            this.weight = weight;
        }

        public String getStr() {
            return str;
        }

        public double getWeight() {
            return weight;
        }

        @Override
        public double score() {
            return weight;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 37 * hash + Objects.hashCode(this.str);
            hash = 37 * hash + (int) (Double.doubleToLongBits(this.weight) ^ (Double.doubleToLongBits(this.weight) >>> 32));
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ScoreableString other = (ScoreableString) obj;
            if (!Objects.equals(this.str, other.str)) {
                return false;
            }
            if (Double.doubleToLongBits(this.weight) != Double.doubleToLongBits(other.weight)) {
                return false;
            }
            return true;
        }
    }

    @Test
    public void basicTest() {

        JedisPool jedisPool = new JedisPool("localhost");

        RedisSortedSet<ScoreableString> set = new RedisSortedSet.RedisSortedSetBuilder<ScoreableString>().jedisPool(jedisPool).name("testSortedSet").build();

        set.clear();

        assertTrue(set.isEmpty());
        assertFalse(set.contains(new ScoreableString("a", 2)));
        assertNotNull(set.toString());

        set.add(new ScoreableString("a", 2));
        assertTrue(set.contains(new ScoreableString("a", 2)));
        assertFalse(set.isEmpty());
        assertEquals(1, set.size());
        assertEquals("a", set.iterator().next().getStr());

        set.remove(new ScoreableString("a", 2));

        assertTrue(set.isEmpty());

        set.add(new ScoreableString("a", 2));
        assertFalse(set.isEmpty());
        assertEquals(1, set.size());
        set.add(new ScoreableString("b", 1));
        assertEquals(2, set.size());
        set.add(new ScoreableString("c", 3));
        assertEquals(3, set.size());
        Iterator<ScoreableString> it = set.iterator();
        assertEquals("b", it.next().getStr());
        assertEquals("a", it.next().getStr());
        assertEquals("c", it.next().getStr());

        List<ScoreableString> list = new ArrayList<>();
        list.add(new ScoreableString("a", 2));
        list.add(new ScoreableString("b", 1));
        assertTrue(set.containsAll(list));

        list = new ArrayList<>();
        list.add(new ScoreableString("a", 2));
        list.add(new ScoreableString("b", 1));
        list.add(new ScoreableString("d", 4));

        set.retainAll(list);
        assertEquals(2, set.size());

        list = new ArrayList<>();
        list.add(new ScoreableString("a", 2));

        set.removeAll(list);
        assertEquals(1, set.size());

        set.clear();

        assertTrue(set.isEmpty());
    }
}
