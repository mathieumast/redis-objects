package org.redis.objects;

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
public class RedisMapTest {

    @Test
    public void basicTest() {

        JedisPool jedisPool = new JedisPool("localhost");

        RedisMap<String, Integer> map = new RedisMap.RedisMapBuilder<String, Integer>().jedisPool(jedisPool).name("testMap").build();
               
        map.clear();

        assertTrue(map.isEmpty());
        assertFalse(map.containsKey("a"));
        assertNotNull(map.toString());

        map.put("a", 1);
        assertTrue(map.containsKey("a"));
        assertFalse(map.isEmpty());
        assertEquals(1, map.size());
        assertEquals(1, (int) map.get("a"));

        map.put("a", 2);
        assertEquals(2, (int) map.get("a"));
        map.remove("a");

        assertTrue(map.isEmpty());

        map.put("a", 1);
        assertFalse(map.isEmpty());
        assertEquals(1, map.size());
        map.put("b", 2);
        assertEquals(2, map.size());
        map.put("c", 3);
        assertEquals(3, map.size());

        map.clear();

        assertTrue(map.isEmpty());

        assertEquals(null, map.get("a"));
        assertEquals(null, map.get("b"));
        assertEquals(null, map.get("c"));
    }
}
