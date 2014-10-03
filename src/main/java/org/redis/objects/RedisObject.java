package org.redis.objects;

import java.io.IOException;
import org.redis.objects.serializer.kryo.KryoSerializer;
import org.redis.objects.serializer.Serializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Redis object.
 *
 * @author Mathieu MAST
 * @param <K>
 * @param <V>
 */
public abstract class RedisObject<K, V> {

    protected final PipelineManager pipelineManager;

    protected final JedisPool jedisPool;

    protected final String name;

    protected boolean syncImmediate = false;

    private final Serializer serializer;

    public RedisObject(final JedisPool jedisPool, final String name, Boolean syncImmediate, Integer maxWithoutSync, Integer delayBeforeSync, Serializer serializer) {
        this.jedisPool = jedisPool;
        this.name = name;
        if (null != syncImmediate) {
            this.syncImmediate = syncImmediate;
        }
        pipelineManager = new PipelineManager(jedisPool);
        if (null != maxWithoutSync) {
            pipelineManager.setMaxWithoutSync(maxWithoutSync);
        }
        if (null != delayBeforeSync) {
            pipelineManager.setMaxWithoutSync(maxWithoutSync);
        }
        if (null != serializer) {
            this.serializer = serializer;
        } else {
            this.serializer = new KryoSerializer();
        }
    }

    protected byte[] valueToBytes(V object) throws IOException {
        return serializer.toBytes(object);
    }

    protected V bytesToValue(byte[] bytes) throws IOException, ClassNotFoundException {
        return (V) serializer.toObject(bytes);
    }

    protected byte[] keyToBytes(K key) throws IOException {
        return serializer.toBytes(key);
    }

    protected K bytesToKey(byte[] bytes) throws IOException, ClassNotFoundException {
        return (K) serializer.toObject(bytes);
    }

    synchronized public void sync() {
        pipelineManager.sync();
    }

    synchronized public <T> T run(Work<T> work) {
        pipelineManager.sync();
        Jedis jedis = jedisPool.getResource();
        try {
            return work.work(jedis);
        } catch (JedisConnectionException ex) {
            jedisPool.returnBrokenResource(jedis);
            throw ex;
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    synchronized public <T> T pipelined(PipelinedWork<T> work) {
        pipelineManager.pipe();
        T res = work.work(pipelineManager.getPipeline());
        if (syncImmediate) {
            pipelineManager.sync();
        } else {
            pipelineManager.delaySync();
        }
        return res;
    }

    public interface Work<T> {

        public T work(Jedis jedis);
    }

    public interface PipelinedWork<T> {

        public T work(Pipeline pipeline);
    }
}
