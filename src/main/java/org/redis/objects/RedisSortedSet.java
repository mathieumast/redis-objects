package org.redis.objects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import lombok.experimental.Builder;
import org.redis.objects.exceptions.RedisobjectsException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.util.SafeEncoder;

/**
 * Redis set.
 *
 * @author Mathieu MAST
 * @param <V>
 */
public class RedisSortedSet<V extends Scoreable> extends RedisObject<V, V> implements Set<V> {

    @Builder
    public RedisSortedSet(final JedisPool jedisPool, final String name, boolean syncImmediate, Integer maxWithoutSync, Integer delayBeforeSync) {
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
                Long l = jedis.zcard(name);
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
    public boolean contains(final Object o) {
        return run(new Work<Boolean>() {

            @Override
            public Boolean work(Jedis jedis) {
                try {
                    Double score = jedis.zscore(SafeEncoder.encode(name), valueToBytes((V) o));
                    return null != score;
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
    public Iterator<V> iterator() {
        return run(new Work<Iterator<V>>() {

            @Override
            public Iterator<V> work(Jedis jedis) {
                try {
                    List<V> list = new ArrayList<>();
                    Set<byte[]> res = jedis.zrange(SafeEncoder.encode(name), 0, -1);
                    for (byte[] bytes : res) {
                        list.add(bytesToValue(bytes));
                    }
                    return list.iterator();
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
    public Object[] toArray() {
        return run(new Work<Object[]>() {

            @Override
            public Object[] work(Jedis jedis) {
                try {
                    List<V> list = new ArrayList<>();
                    Set<byte[]> res = jedis.zrange(SafeEncoder.encode(name), 0, -1);
                    for (byte[] bytes : res) {
                        list.add(bytesToValue(bytes));
                    }
                    return list.toArray();
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
    public <T> T[] toArray(final T[] a) {
        return run(new Work<T[]>() {

            @Override
            public T[] work(Jedis jedis) {
                try {
                    List<V> list = new ArrayList<>();
                    Set<byte[]> res = jedis.zrange(SafeEncoder.encode(name), 0, -1);
                    for (byte[] bytes : res) {
                        list.add(bytesToValue(bytes));
                    }
                    return list.toArray(a);
                } catch (IOException | ClassNotFoundException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * Add object (WARNING: always returning true).
     *
     * @param e object
     * @return true
     */
    @Override
    public boolean add(final V e) {
        return pipelined(new PipelinedWork<Boolean>() {

            @Override
            public Boolean work(Pipeline pipeline) {
                try {
                    pipeline.zadd(SafeEncoder.encode(name), e.score(), valueToBytes((V) e));
                    return true;
                } catch (IOException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * Remove object (WARNING: always returning true).
     *
     * @return true
     */
    @Override
    public boolean remove(final Object o) {
        return pipelined(new PipelinedWork<Boolean>() {

            @Override
            public Boolean work(Pipeline pipeline) {
                try {
                    pipeline.zrem(SafeEncoder.encode(name), valueToBytes((V) o));
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
    public boolean containsAll(final Collection<?> c) {
        return run(new Work<Boolean>() {

            @Override
            public Boolean work(Jedis jedis) {
                try {
                    for (Object o : c) {
                        Double score = jedis.zscore(SafeEncoder.encode(name), valueToBytes((V) o));
                        if (null == score) {
                            return false;
                        }
                    }
                    return true;
                } catch (IOException ex) {
                    throw new RedisobjectsException(ex);
                }
            }
        });
    }

    /**
     * Add objects (WARNING: always returning true).
     *
     * @param c objects
     * @return true
     */
    @Override
    public boolean addAll(final Collection<? extends V> c) {
        return run(new Work<Boolean>() {

            @Override
            public Boolean work(Jedis jedis) {
                Transaction tr = jedis.multi();
                try {
                    for (V value : c) {
                        tr.zadd(SafeEncoder.encode(name), ((Scoreable) value).score(), valueToBytes((V) value));
                    }
                    tr.exec();
                } catch (IOException ex) {
                    tr.discard();
                    throw new RedisobjectsException(ex);
                }
                return true;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean retainAll(final Collection<?> c) {
        return run(new Work<Boolean>() {

            @Override
            public Boolean work(Jedis jedis) {
                boolean res = false;
                Set<byte[]> list = jedis.zrange(SafeEncoder.encode(name), 0, -1);
                Transaction tr = jedis.multi();
                try {
                    for (byte[] bytes : list) {
                        V value = (V) bytesToValue(bytes);
                        if (!c.contains(value)) {
                            tr.zrem(SafeEncoder.encode(name), bytes);
                            res = true;
                        }
                    }
                    tr.exec();
                } catch (IOException | ClassNotFoundException ex) {
                    tr.discard();
                    throw new RedisobjectsException(ex);
                }
                return res;
            }
        });
    }

    /**
     * Remove objects (WARNING: always returning true).
     *
     * @param c objects
     * @return true
     */
    @Override
    public boolean removeAll(final Collection<?> c) {
        return run(new Work<Boolean>() {

            @Override
            public Boolean work(Jedis jedis) {
                Transaction tr = jedis.multi();
                try {
                    for (Object object : c) {
                        tr.zrem(SafeEncoder.encode(name), valueToBytes((V) object));
                    }
                    tr.exec();
                } catch (IOException ex) {
                    tr.discard();
                    throw new RedisobjectsException(ex);
                }
                return true;
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
}
