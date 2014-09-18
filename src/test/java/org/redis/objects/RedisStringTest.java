package org.redis.objects;

import static junit.framework.TestCase.assertEquals;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Mathieu MAST
 */
public class RedisStringTest {

    @Test
    public void basicTest() {

        JedisPool jedisPool = new JedisPool("localhost");

        StringBuilder str = new StringBuilder();
        RedisString strRedis = new RedisString.RedisStringBuilder().jedisPool(jedisPool).name("testString").build();
        
        assertEquals(0, str.length());
        assertEquals(0, strRedis.length());
        assertEquals("", str.toString());
        assertEquals("", strRedis.toString());

        strRedis.clear();

        str.append("hello");
        strRedis.append("hello");

        assertEquals(5, str.length());
        assertEquals(5, strRedis.length());

        assertEquals("lo", str.subSequence(3, 5));
        assertEquals("lo", strRedis.subSequence(3, 5));

        str.append("é$€ïüâ§");
        strRedis.append("é$€ïüâ§");

        assertEquals("helloé$€ïüâ§", str.toString());
        assertEquals("helloé$€ïüâ§", strRedis.toString());

        assertEquals("ïüâ", str.subSequence(8, 11));
        assertEquals("ïüâ", strRedis.subSequence(8, 11));

        assertEquals(12, str.length());
        assertEquals(12, strRedis.length());

        strRedis.clear();

        assertEquals(0, strRedis.length());
    }
}
