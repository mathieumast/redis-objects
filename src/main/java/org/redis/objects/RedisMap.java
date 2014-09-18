package org.redis.objects;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.experimental.Builder;
import org.redis.objects.exceptions.RedisobjectsException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.util.SafeEncoder;

/**
 * Redis map.
 *
 * @author Mathieu MAST
 * @param <K>
 * @param <V>
 */
public class RedisMap<K, V> extends RedisObject<K, V> implements Map<K, V> {

    @Builder
    public RedisMap(final JedisPool jedisPool, final String name, boolean syncImmediate, Integer maxWithoutSync, Integer delayBeforeSync) {
        super(jedisPool, name, syncImmediate, maxWithoutSync, delayBeforeSync);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return run(new Work<Integer>() {

            @Override
            public Integer work(Jedis jedis) {
                Long l = jedis.hlen(name);
                if (null == l) {
                    return 0;
                } else {
                    return l.intValue();
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return 0 == size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(final Object key) {
        return run(new Work<Boolean>() {
            
            @Override
            public Boolean work(Jedis jedis) {
                try {
                    Boolean exists = jedis.hexists(SafeEncoder.encode(name), keyToBytes((K) key));
                    if (null == exists) {
                        return false;
                    }
                    return exists;
                } catch (IOException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(final Object value) {
        return run(new Work<Boolean>() {

            @Override
            public Boolean work(Jedis jedis) {
                try {
                    Set<byte[]> res = jedis.hkeys(SafeEncoder.encode(name));
                    if (null == res) {
                        return false;
                    }
                    return res.contains(valueToBytes((V) value));
                } catch (IOException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(final Object key) {
        return run(new Work<V>() {

            @Override
            public V work(Jedis jedis) {
                try {
                    byte[] bytes = jedis.hget(SafeEncoder.encode(name), keyToBytes((K) key));
                    if (null == bytes) {
                        return null;
                    }
                    return bytesToValue(bytes);
                } catch (IOException | ClassNotFoundException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * Put object for key (WARNING: always returning null).
     *
     * @param key key
     * @param value object
     * @return null
     */
    @Override
    public V put(final K key, final V value) {
        return pipelined(new PipelinedWork<V>() {

            @Override
            public V work(Pipeline pipeline) {
                try {
                    pipeline.hset(SafeEncoder.encode(name), keyToBytes((K) key), valueToBytes((V) value));
                    return null;
                } catch (IOException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * Remove object stored with the key (WARNING: always returning null).
     *
     * @param key key
     * @return null
     */
    @Override
    public V remove(final Object key) {
        return pipelined(new PipelinedWork<V>() {

            @Override
            public V work(Pipeline pipeline) {
                try {
                    pipeline.hdel(SafeEncoder.encode(name), keyToBytes((K) key));
                    return null;
                } catch (IOException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        pipelined(new PipelinedWork<Boolean>() {

            @Override
            public Boolean work(Pipeline pipeline) {
                try {
                    Map<byte[], byte[]> map = new HashMap<>();
                    for (java.util.Map.Entry<? extends K, ? extends V> en : m.entrySet()) {
                        Object key = en.getKey();
                        Object val = en.getValue();
                        map.put(keyToBytes((K) key), valueToBytes((V) val));
                    }
                    pipeline.hmset(SafeEncoder.encode(name), map);
                    return true;
                } catch (IOException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        pipelined(new PipelinedWork<Boolean>() {

            @Override
            public Boolean work(Pipeline pipeline) {
                pipeline.del(SafeEncoder.encode(name));
                return true;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<K> keySet() {
        return run(new Work<Set<K>>() {

            @Override
            public Set<K> work(Jedis jedis) {
                try {
                    Set<K> set = new HashSet<>();
                    Set<byte[]> res = jedis.hkeys(SafeEncoder.encode(name));
                    for (byte[] bytes : res) {
                        set.add(bytesToKey(bytes));
                    }
                    return set;
                } catch (IOException | ClassNotFoundException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<V> values() {
        return run(new Work<List<V>>() {

            @Override
            public List<V> work(Jedis jedis) {
                try {
                    List<V> list = new ArrayList<>();
                    List<byte[]> res = jedis.hvals(SafeEncoder.encode(name));
                    for (byte[] bytes : res) {
                        list.add(bytesToValue(bytes));
                    }
                    return list;
                } catch (IOException | ClassNotFoundException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * Get values for keys.
     *
     * @param keys keys
     * @return values
     */
    public Collection<V> values(final Collection<K> keys) {
        return run(new Work<List<V>>() {

            @Override
            public List<V> work(Jedis jedis) {
                try {
                    List<V> list = new ArrayList<>();
                    byte[][] safeKeys = new byte[keys.size()][];
                    int i = 0;
                    for (K key : keys) {
                        safeKeys[i] = keyToBytes((K) key);
                        i++;
                    }
                    List<byte[]> res = jedis.hmget(SafeEncoder.encode(name), safeKeys);
                    for (byte[] bytes : res) {
                        list.add(bytesToValue(bytes));
                    }
                    return list;
                } catch (IOException | ClassNotFoundException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return run(new Work<Set<Entry<K, V>>>() {

            @Override
            public Set<Entry<K, V>> work(Jedis jedis) {
                try {
                    Set<Entry<K, V>> set = new HashSet<>();
                    Map<byte[], byte[]> res = jedis.hgetAll(SafeEncoder.encode(name));
                    for (Entry<byte[], byte[]> entry : res.entrySet()) {
                        byte[] bskey = entry.getKey();
                        byte[] bsval = entry.getValue();
                        set.add(new AbstractMap.SimpleEntry(bytesToKey(bskey), bytesToValue(bsval)));
                    }
                    return set;
                } catch (IOException | ClassNotFoundException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }
}
