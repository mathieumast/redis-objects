package org.redis.objects.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.redis.objects.serializer.Serializer;

/**
 *
 * @author Mathieu MAST
 */
public class KryoSerializer implements Serializer {
    
    private final KryoPool kryoPool = KryoPool.getInstance();

    @Override
    public byte[] toBytes(Object object) throws IOException {
        Kryo kryo = kryoPool.get();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, object);
            output.close();
            return baos.toByteArray();
        } finally {
            kryoPool.release(kryo);
        }
    }

    @Override
    public Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        Kryo kryo = kryoPool.get();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); Input input = new Input(bais)) {
            return kryo.readClassAndObject(input);
        } finally {
            kryoPool.release(kryo);
        }
    }

}
