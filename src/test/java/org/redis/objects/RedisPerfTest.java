package org.redis.objects;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static junit.framework.TestCase.assertEquals;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Mathieu MAST
 */
public class RedisPerfTest {

    @Test
    public void perfMapSyncImmediate() {
        JedisPool jedisPool = new JedisPool("localhost");

        RedisMap<String, String> map = new RedisMap.RedisMapBuilder<String, String>().jedisPool(jedisPool).name("testMapPerfSyncImmediate").syncImmediate(true).build();

        map.clear();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            String s = UUID.randomUUID().toString();
            map.put(s, s);
        }
        long end = System.currentTimeMillis();

        assertEquals(100000, map.size());

        System.out.println("perfMapSyncImmediate - time (ms): " + (end - start));

        map.clear();
    }

    @Test
    public void perfMap() {
        JedisPool jedisPool = new JedisPool("localhost");

        RedisMap<String, String> map = new RedisMap.RedisMapBuilder<String, String>().jedisPool(jedisPool).name("testMapPerf").build();

        map.clear();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            String s = UUID.randomUUID().toString();
            map.put(s, s);
        }
        long end = System.currentTimeMillis();

        assertEquals(100000, map.size());

        System.out.println("perfMap - time (ms): " + (end - start));

        map.clear();
    }

    @Test
    public void perfMapMultiThread() throws InterruptedException {
        JedisPool jedisPool = new JedisPool("localhost");

        final RedisMap<String, String> map = new RedisMap.RedisMapBuilder<String, String>().jedisPool(jedisPool).name("testMapPerfMultiThread").build();

        map.clear();

        long start = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 10; i++) {

            pool.submit(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 10000; j++) {
                        String s = UUID.randomUUID().toString();
                        map.put(s, s);
                    }
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);

        long end = System.currentTimeMillis();

        assertEquals(100000, map.size());

        System.out.println("perfMapMultiThread - time (ms): " + (end - start));

        map.clear();
    }
}
