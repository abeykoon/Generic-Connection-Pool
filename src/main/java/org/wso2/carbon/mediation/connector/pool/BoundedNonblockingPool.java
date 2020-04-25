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

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;

/**
 * Implementation of non-blocking bounded pool
 */
public class BoundedNonblockingPool<T> extends AbstractGenericPool <T> {

    //Size of the pool
    private int size;

    //Keeps pooled connections
    private Queue<T> connections;

    private ConnectionValidator <T> validator;
    private ConnectionFactory <T> connectionFactory;

    //TODO: understand why a Semaphore is used and if init is OK
    private Semaphore permits = new Semaphore(1);

    private volatile boolean shutdownCalled;


    public BoundedNonblockingPool(int size, ConnectionValidator<T> validator, ConnectionFactory<T> connectionFactory) {

        super();

        this.connectionFactory = connectionFactory;
        this.size = size;
        this.validator = validator;

        connections = new LinkedList<T>();

        initializeObjects();

        shutdownCalled = false;
    }

    public T get() {
        T connection = null;

        if (!shutdownCalled) {
            if (permits.tryAcquire()) {
                connection = connections.poll();
            }
        } else {
            throw new IllegalStateException(
                    "Object pool already shutdown");
        }
        return connection;
    }

    public void shutdown() {
        shutdownCalled = true;
        clearResources();
    }

    @Override
    protected void handleInvalidReturn(T connection) {

    }

    @Override
    protected void returnToPool(T connection) {
        boolean added = connections.add(connection);
        if (added) {
            permits.release();
        }
    }

    @Override
    protected boolean isValid(T connection) {
        return validator.isValid(connection);
    }


    /**
     * Create new connections and populate the pool
     */
    private void initializeObjects() {
        for (int i = 0; i < size; i++) {
            connections.add(connectionFactory.createNew());
        }
    }

    private void clearResources() {
        for (T connection : connections) {
            validator.invalidate(connection);
        }
    }
}
