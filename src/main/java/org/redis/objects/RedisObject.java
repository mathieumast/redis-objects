package org.redis.objects;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.jboss.serial.io.JBossObjectInputStream;
import org.jboss.serial.io.JBossObjectOutputStream;
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
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); JBossObjectOutputStream jbos = new JBossObjectOutputStream(baos)) {
            jbos.writeObject(object);
            return baos.toByteArray();
        }
    }

    protected V bytesToValue(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); JBossObjectInputStream jbis = new JBossObjectInputStream(bais)) {
            return (V) jbis.readObject();
        }
    }

    protected byte[] keyToBytes(K key) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); JBossObjectOutputStream jbos = new JBossObjectOutputStream(baos)) {
            jbos.writeObject(key);
            return baos.toByteArray();
        }
    }

    protected K bytesToKey(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); JBossObjectInputStream jbis = new JBossObjectInputStream(bais)) {
            return (K) jbis.readObject();
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
