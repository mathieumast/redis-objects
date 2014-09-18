package org.redis.objects.exceptions;

/**
 * Redisobjects exception.
 *
 * @author Mathieu MAST
 */
public class RedisobjectsException extends RuntimeException {

    public RedisobjectsException(Throwable cause) {
        super(cause);
    }
    
    public RedisobjectsException(String message) {
        super(message);
    }
}
