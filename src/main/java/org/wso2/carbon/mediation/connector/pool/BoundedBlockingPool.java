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

import org.wso2.carbon.mediation.connector.utils.PoolLog;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link BlockingPool}
 * Fixed size connection pool.
 * Connection obtain call will be blocked
 * if no connection is available until an existing
 * connection returns to the pool or timeout.
 */
public final class BoundedBlockingPool<T> extends AbstractGenericPool<T> implements BlockingPool<T> {

    private PoolLog log;

    //Name of the pool
    private String name;

    //Size of the pool
    private int size;

    //Params used for resiliency of connection creation
    private ConnectionCreatorResiliencyParams resiliencyParams;

    //Internal queue to store connections
    private BlockingQueue<T> connections;

    private ConnectionValidator<T> validator;

    private ConnectionFactory<T> connectionFactory;

    //Thread pool for executing returns of used connections.
    private ExecutorService connectionReturnerExecutor =
            Executors.newCachedThreadPool();

    /**
     * Thread pool for executing connection creations.
     * This is single threaded because even if multiple
     * connection creation jobs are submitted, without one
     * being successful, others will never succeed (i.e backend is down)
     */
    private ExecutorService connectionCreatorExecutor =
            Executors.newSingleThreadExecutor();

    private volatile boolean shutdownCalled;


    /**
     * Create a new BoundedBlockingPool. This is a fixed size thread pool. Connection requests will
     * get blocked until a connection is returned (optionally with a timeout).
     *
     * @param name              Name of the pool
     * @param size              Size of the pool
     * @param resiliencyParams  Parameters related to connection retry.
     * @param validator         {ConnectionValidator} instance containing
     *                          logic how to validate connections
     * @param connectionFactory {ConnectionFactory} instance containing
     *                          logic how to create connections
     */
    public BoundedBlockingPool(String name,
                               int size,
                               ConnectionCreatorResiliencyParams resiliencyParams,
                               ConnectionValidator<T> validator,
                               ConnectionFactory<T> connectionFactory) {

        super();

        log = new PoolLog(name, BoundedBlockingPool.class);
        this.name = name;
        this.size = size;
        this.resiliencyParams = resiliencyParams;
        this.connectionFactory = connectionFactory;
        this.validator = validator;

        connections = new LinkedBlockingQueue<T>(size);

        initializeConnections();

        shutdownCalled = false;
    }

    public T get(long timeOut, TimeUnit unit) throws InterruptedException {
        if (log.isDebugOn()) {
            log.debug("Connection get request submitted. Thread id = " + Thread.currentThread().getId());
        }
        if (!shutdownCalled) {
            T connection = null;
            boolean timeUtilized = false;
            try {
                connection = connections.poll();
                if (connection == null) {
                    connection = connections.poll(timeOut, unit);
                    timeUtilized = true;
                }
                if (isValid(connection)) {
                    return connection;
                } else {
                    if (log.isDebugOn()) {
                        log.debug("Test on borrow failed for connection");
                    }
                    if (timeUtilized) {
                        return null;
                    } else {
                        handleInvalidConnection(connection);
                        //recursively try to get a connection
                        return get(timeOut, unit);
                    }
                }

            } catch (InterruptedException ie) {
                log.error("Application thread " + Thread.currentThread().getId() + "is interrupted.", ie);
                Thread.currentThread().interrupt();
            }
            return connection;
        }
        throw new IllegalStateException("Connection pool is already shutdown");
    }

    public T get() {
        if (log.isDebugOn()) {
            log.debug("Connection get request submitted. Thread id = " + Thread.currentThread().getId());
        }
        return testAndGetConnection();
    }

    @Override
    protected void handleInvalidReturn(T invalidConnection) {
        handleInvalidConnection(invalidConnection);
    }

    @Override
    protected void returnToPool(T connection) {
        connectionReturnerExecutor.submit(new ConnectionReturner(connections, connection));
    }

    @Override
    protected boolean isValid(T connection) {
        return validator.isValid(connection);
    }

    public void shutdown() {
        log.info("Shutting down connection pool");
        shutdownCalled = true;
        connectionReturnerExecutor.shutdownNow();
        connectionCreatorExecutor.shutdownNow();
        clearResources();
    }

    /**
     * Create connections up to
     * the initial size of the pool
     */
    private void initializeConnections() {
        for (int i = 0; i < size; i++) {
            connections.add(connectionFactory.createNew());
        }
        if (log.isDebugOn()) {
            log.debug("Connection pool initialized with " + size + " connections.");
        }
    }

    /**
     * Test and get a connection. This will only return a validated connection.
     * It will recursively try all connections in the pool until it meets one.
     * Same time, it will handle failed connections.
     *
     * @return Validated connection
     */
    private T testAndGetConnection() {
        if (log.isDebugOn()) {
            log.debug("Trying to get a connection from pool.");
        }
        if (!shutdownCalled) {
            T connection = null;
            try {
                connection = connections.take();
            } catch (InterruptedException ie) {
                log.error("Thread " + Thread.currentThread().getId() + " could not obtain a connection", ie);
                Thread.currentThread().interrupt();
            }
            if (isValid(connection)) {
                return connection;
            } else {
                if (log.isDebugOn()) {
                    log.debug("Test on borrow failed for connection");
                }
                handleInvalidConnection(connection);
                //recursively try to get a connection
                return testAndGetConnection();
            }
        }
        throw new IllegalStateException("Object pool is already shutdown");
    }

    /**
     * Handle connection when connection validation fails.
     * Need to give up resources held by connection and
     * asynchronously try to create a new connection in place
     * of invalid one.
     *
     * @param connection invalid connection
     */
    private void handleInvalidConnection(T connection) {
        //TODO: this may throw exceptions
        validator.invalidate(connection);
        requestNewConnectionCreate();
    }

    /**
     * Request to create a new connection and add to the pool.
     * The invoking thread will not get blocked until connection creation.
     * It will return immediately.
     */
    private void requestNewConnectionCreate() {
        connectionCreatorExecutor.submit(new ConnectionCreator(name, new BlockingPoolConnectionAdder(), connectionFactory,
                resiliencyParams));
        if (log.isDebugOn()) {
            log.debug("A connection creation request submitted");
        }
    }

    /**
     * Clear all resources of connection pool.
     * Connections are closed. Sockets are released.
     */
    private void clearResources() {
        for (T connection : connections) {
            validator.invalidate(connection);
        }
    }

    /**
     * Represents how connection inserts are done to the pool
     */
    private class BlockingPoolConnectionAdder implements PoolConnectionAdder<T> {

        public boolean add(T connection) {
            boolean success = connections.add(connection);
            if (success) {
                if (log.isDebugOn()) {
                    log.debug("Added created connection to the pool. "
                            + "Current pool size= " + connections.size());
                }
            } else {
                if (log.isDebugOn()) {
                    log.debug("Could not add the created connection to the pool. "
                            + "Current pool size= " + connections.size());
                }
            }
            return success;
        }
    }


    /**
     * Represents class for returning connections back to the pool.
     * These are short-lived asynchronous tasks. Idea is the application
     * thread trying to return the connection should not be blocked
     * if the queue is full.
     *
     * @param <E> an instance of the Connection
     */
    private class ConnectionReturner<E> implements Callable<Void> {
        private BlockingQueue<E> queue;
        private E connectionToReturn;

        ConnectionReturner(BlockingQueue<E> queue, E connectionToReturn) {
            this.queue = queue;
            this.connectionToReturn = connectionToReturn;
        }

        public Void call() {
            while (true) {
                try {
                    //this call will wait if there's no space available
                    queue.put(connectionToReturn);
                    break;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            return null;
        }
    }

}
