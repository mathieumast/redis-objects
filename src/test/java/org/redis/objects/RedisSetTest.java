package org.redis.objects;

import java.util.ArrayList;
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
public class RedisSetTest {

    @Test
    public void basicTest() {

        JedisPool jedisPool = new JedisPool("localhost");

        RedisSet<String> set = new RedisSet.RedisSetBuilder<String>().jedisPool(jedisPool).name("testSet").build();

        set.clear();

        assertTrue(set.isEmpty());
        assertFalse(set.contains("a"));
        assertNotNull(set.toString());

        set.add("a");
        assertTrue(set.contains("a"));
        assertFalse(set.isEmpty());
        assertEquals(1, set.size());
        assertEquals("a", set.iterator().next());

        set.remove("a");

        assertTrue(set.isEmpty());

        set.add("a");
        assertFalse(set.isEmpty());
        assertEquals(1, set.size());
        set.add("b");
        assertEquals(2, set.size());
        set.add("c");
        assertEquals(3, set.size());
        
        assertEquals(3, set.toArray().length);
        assertEquals(3, set.toArray(new String[0]).length);

        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        assertTrue(set.containsAll(list));

        set.removeAll(list);
        assertEquals(1, set.size());

        set.clear();

        assertTrue(set.isEmpty());
    }
}
