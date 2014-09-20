package org.redis.objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    private KryoPool kryoPool = new KryoPool();
    
    public RedisObject(final JedisPool jedisPool, final String name, Boolean syncImmediate, Integer maxWithoutSync, Integer delayBeforeSync) {
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
    }

    protected byte[] valueToBytes(V object) throws IOException {
        Kryo kryo = kryoPool.get();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, object);
            output.close();
            return baos.toByteArray();
        } finally {
            kryoPool.release(kryo);
        }
    }

    protected V bytesToValue(byte[] bytes) throws IOException, ClassNotFoundException {
        Kryo kryo = kryoPool.get();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); Input input = new Input(bais)) {
            return (V) kryo.readClassAndObject(input);
        } finally {
            kryoPool.release(kryo);
        }
    }

    protected byte[] keyToBytes(K key) throws IOException {
        Kryo kryo = kryoPool.get();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, key);
            output.close();
            return baos.toByteArray();
        } finally {
            kryoPool.release(kryo);
        }
    }

    protected K bytesToKey(byte[] bytes) throws IOException, ClassNotFoundException {
        Kryo kryo = kryoPool.get();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); Input input = new Input(bais)) {
            return (K) kryo.readClassAndObject(input);
        } finally {
            kryoPool.release(kryo);
        }
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
