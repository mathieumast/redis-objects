package org.redis.objects.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Kryo pool.
 *
 * @author Mathieu MAST
 */
public class KryoPool {

    private static final KryoPool InternalKryoPool = new KryoPool();

    private final Queue<Kryo> kryos = new ConcurrentLinkedQueue<>();

    private KryoPool() {
    }

    public static KryoPool getInstance() {
        return InternalKryoPool;
    }

    public Kryo get() {
        Kryo kryo;
        if ((kryo = kryos.poll()) == null) {
            kryo = createInstance();
        }
        return kryo;
    }

    public void release(Kryo kryo) {
        kryos.offer(kryo);
    }

    private Kryo createInstance() {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        return kryo;
    }
}
