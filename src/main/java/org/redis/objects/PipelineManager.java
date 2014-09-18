package org.redis.objects;

import java.util.Timer;
import java.util.TimerTask;
import lombok.Getter;
import lombok.Setter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Pipeline Manager.
 *
 * @author Mathieu MAST
 */
public class PipelineManager {

    private final JedisPool jedisPool;

    @Getter
    @Setter
    private int maxWithoutSync = 100;

    @Getter
    @Setter
    private int delayBeforeSync = 100;

    private long count = 0;

    private Jedis jedis = null;

    @Getter
    private Pipeline pipeline = null;

    private final Timer timer = new Timer();

    public PipelineManager(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    synchronized public void pipe() {
        if (0 == count % maxWithoutSync) {
            sync();
        }
        if (null == pipeline) {
            jedis = jedisPool.getResource();
            pipeline = jedis.pipelined();
        }
        count++;
    }

    synchronized public void sync() {
        if (null == pipeline) {
            return;
        }
        try {
            pipeline.sync();
        } catch (JedisConnectionException ex) {
            jedisPool.returnBrokenResource(jedis);
            throw ex;
        } finally {
            jedisPool.returnResource(jedis);
            jedis = null;
            pipeline = null;
        }
    }

    synchronized public void delaySync() {
        if (null != pipeline) {
            return;
        }
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (null != pipeline) {
                    sync();
                }
            }
        }, delayBeforeSync);
    }
}
