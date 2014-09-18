package org.redis.objects;

import lombok.experimental.Builder;
import org.redis.objects.exceptions.RedisobjectsException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

/**
 * Redis string.
 *
 * @author Mathieu MAST
 */
public class RedisString extends RedisObject<String, String> implements CharSequence {

    @Builder
    public RedisString(final JedisPool jedisPool, final String name, boolean syncImmediate, Integer maxWithoutSync, Integer delayBeforeSync) {
        super(jedisPool, name, syncImmediate, maxWithoutSync, delayBeforeSync);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int length() {
        return run(new Work<Integer>() {

            @Override
            public Integer work(Jedis jedis) {
                String res = jedis.get(name);
                if (null == res) {
                    return 0;
                } else {
                    return res.length();
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public char charAt(final int index) {
        return run(new Work<Character>() {

            @Override
            public Character work(Jedis jedis) {
                String res = jedis.get(name);
                if (null == res) {
                    throw new RedisobjectsException("String not found in Redis");
                } else {
                    return res.charAt(index);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CharSequence subSequence(final int start, final int end) {
        return run(new Work<CharSequence>() {

            @Override
            public CharSequence work(Jedis jedis) {
                String res = jedis.get(name);
                if (null == res) {
                    throw new RedisobjectsException("String not found in Redis");
                } else {
                    return res.subSequence(start, end);
                }
            }
        });
    }

    /**
     * Append to string stored in Redis.
     *
     * @param s string
     * @return this
     */
    public RedisString append(final String s) {
        pipelined(new PipelinedWork<Boolean>() {

            @Override
            public Boolean work(Pipeline pipeline) {
                pipeline.append(name, s);
                return true;
            }
        });

        return this;
    }

    public RedisString clear() {
        pipelined(new PipelinedWork<Boolean>() {

            @Override
            public Boolean work(Pipeline pipeline) {
                pipeline.del(name);
                return true;
            }
        });

        return this;
    }

    /**
     * Return the string stored in Redis.
     *
     * @return string
     */
    @Override
    public String toString() {
        return run(new Work<String>() {

            @Override
            public String work(Jedis jedis) {
                String res = jedis.get(name);
                if (null == res) {
                    return "";
                } else {
                    return res;
                }
            }
        });
    }
}
