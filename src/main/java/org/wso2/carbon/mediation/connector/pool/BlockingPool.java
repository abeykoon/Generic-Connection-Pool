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

import java.util.concurrent.TimeUnit;

/**
 * Represents a pool of objects that makes the
 * requesting threads wait if no object is available.
 *
 * @param <T> the type of objects to pool.
 */
public interface BlockingPool<T> extends Pool<T> {
    /**
     * Returns an instance of type T from the pool.
     * <p>
     * The call is a blocking call,
     * and client threads are made to wait
     * indefinitely until an object is available.
     * The call implements a fairness algorithm
     * that ensures that a FCFS service is implemented.
     * <p>
     * Clients are advised to react to InterruptedException.
     * If the thread is interrupted while waiting
     * for an object to become available,
     * the current implementations
     * sets the interrupted state of the thread
     * to <code>true</code> and returns null.
     * However this is subject to change
     * from implementation to implementation.
     *
     * @return T an instance of the Object
     * of type T from the pool.
     */
    T get();

    /**
     * Returns an instance of type T from the pool,
     * waiting up to the
     * specified wait time if necessary
     * for an object to become available..
     * <p>
     * The call is a blocking call,
     * and client threads are made to wait
     * for time until an object is available
     * or until the timeout occurs.
     * The call implements a fairness algorithm
     * that ensures that a FCFS service is implemented.
     * <p>
     * Clients are advised to react to InterruptedException.
     * If the thread is interrupted while waiting
     * for an object to become available,
     * the current implementations
     * set the interrupted state of the thread
     * to <code>true</code> and returns null.
     * However this is subject to change
     * from implementation to implementation.
     *
     * @param time amount of time to wait before giving up,
     *             in units of <tt>unit</tt>
     * @param unit a <tt>TimeUnit</tt> determining
     *             how to interpret the
     *             <tt>timeout</tt> parameter
     * @return T an instance of the Object
     * of type T from the pool.
     * @throws InterruptedException if interrupted while waiting
     */

    T get(long time, TimeUnit unit) throws InterruptedException;
}
