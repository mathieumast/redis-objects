package org.redis.objects;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    public class ScoreableString implements Scoreable, Serializable {

        private String str;

        private double weight;

        public ScoreableString(String str, double weight) {
            this.str = str;
            this.weight = weight;
        }

        public String getStr() {
            return str;
        }

        public void setStr(String str) {
            this.str = str;
        }

        public double getWeight() {
            return weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }

        @Override
        public double score() {
            return weight;
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

        set.removeAll(list);
        assertEquals(1, set.size());

        set.clear();

        assertTrue(set.isEmpty());
    }
}
