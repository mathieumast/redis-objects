package org.redis.objects.serializer;

import java.io.IOException;

/**
 *
 * @author Mathieu MAST
 */
public interface Serializer {

    byte[] toBytes(Object object) throws IOException;

    Object toObject(byte[] bytes) throws IOException, ClassNotFoundException;
}
