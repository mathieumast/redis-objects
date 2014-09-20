package org.redis.objects;

import com.esotericsoftware.kryo.Kryo;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Kryo pool.
 *
 * @author Mathieu MAST
 */
public class KryoPool {

    private final Queue<Kryo> kryos = new ConcurrentLinkedQueue<>();

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
