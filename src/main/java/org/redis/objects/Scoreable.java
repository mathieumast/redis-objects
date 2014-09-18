package org.redis.objects;

/**
 * Scoreable object.
 *
 * @author Mathieu MAST
 */
public interface Scoreable {

    /**
     * Compute score.
     *
     * @return score
     */
    double score();
}
