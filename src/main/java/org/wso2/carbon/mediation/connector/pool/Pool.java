/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.mediation.connector.pool;

/**
 * Represents a cached pool of objects.
 *
 * @param <T> the type of object to pool.
 */
public interface Pool<T> {

    /**
     * Returns an instance from the pool.
     * The call may be a blocking one or a non-blocking one
     * and that is determined by the internal implementation.
     * <p>
     * If the call is a blocking call,
     * the call returns immediately with a valid object
     * if available, else the thread is made to wait
     * until an object becomes available.
     * In case of a blocking call,
     * it is advised that clients react
     * to {@link InterruptedException} which might be thrown
     * when the thread waits for an object to become available.
     * <p>
     * If the call is a non-blocking one,
     * the call returns immediately irrespective of
     * whether an object is available or not.
     * If any object is available the call returns it
     * else the call returns <code>null</code>.
     * <p>
     * The validity of the objects are determined using the
     * {@link ConnectionValidator} interface, such that
     * an object <code>o</code> is valid if
     * <code> ConnectionValidator.isValid(o) == true </code>.
     *
     * @return T one of the pooled objects.
     */
    T get();

    /**
     * Releases the object and puts it back to the pool.
     * <p>
     * The mechanism of putting the object back to the pool is
     * generally asynchronous,
     * however future implementations might differ.
     *
     * @param t the object to return to the pool
     */
    void release(T t);

    /**
     * Shuts down the pool. In essence this call will not
     * accept any more requests
     * and will release all resources.
     * Releasing resources are done
     * via the < code >invalidate()< /code >
     * method of the {@link ConnectionValidator} interface.
     */
    void shutdown();

    /**
     * Represents the functionality to
     * validate an object of the pool
     * and to subsequently perform cleanup activities.
     *
     * @param <T> the type of objects to validate and cleanup.
     */
    interface ConnectionValidator<T> {
        /**
         * Checks whether the object is valid.
         *
         * @param t the object to check.
         * @return <code>true</code>
         * if the object is valid else <code>false</code>.
         */
        boolean isValid(T t);

        /**
         * Performs any cleanup activities
         * before discarding the object.
         * For example before discarding
         * database connection objects,
         * the pool will want to close the connections.
         * This is done via the
         * <code>invalidate()</code> method.
         * <p>
         * NOTE: Make sure this does not throw exceptions.
         * Implementation should handle/log any errors
         * </p>
         *
         * @param t the object to cleanup
         */
        void invalidate(T t);

        /**
         * Get Id of the connection.
         *
         * @param t the object to check
         * @return Id of the object
         */
        String getConnectionId(T t);
    }

    /**
     * Helper class to pass ConnectionCreator Resiliency parameters
     */
    class ConnectionCreatorResiliencyParams {
        /**
         * @param initialInterval Base interval (seconds). Retry will start from this interval
         * @param multiplier      Interval will increase by this factor for each try
         * @param maxRetries      Max number of times to retry
         * @param maxInterval     Max interval (seconds) between two retries. Exponential increase of
         *                        interval will be limited by this
         */
        public ConnectionCreatorResiliencyParams(int initialInterval, float multiplier, int maxRetries, int maxInterval) {
            this.initialInterval = initialInterval;
            this.multiplier = multiplier;
            this.maxRetries = maxRetries;
            this.maxInterval = maxInterval;
        }

        int initialInterval;

        float multiplier;

        int maxRetries;

        int maxInterval;
    }

}
