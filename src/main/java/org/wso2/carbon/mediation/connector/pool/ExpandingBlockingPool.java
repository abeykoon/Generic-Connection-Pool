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

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of {@link BlockingPool}
 * Pool has an initial initialSize.
 * If all connections are utilized, pool will expand up to a max initialSize.
 * If additional connections are idle they will get removed and pool
 * shrink back to initial initialSize.
 * After max initialSize is reached, if a new connection request comes, it
 * will wait get blocked until an existing connection is released or timeout
 */
public final class ExpandingBlockingPool<T> extends AbstractGenericPool<T> implements BlockingPool<T> {

    private PoolLog log;

    //Name of the pool
    private String name;

    //Initial size of the pool
    private int initialSize;

    //Maximum size of the connection pool
    private int maxSize;

    /**
     * Permit for creating a new connection (represents open sockets)
     * <code>open sockets = connections in the pool + connections acquired by apps</code>
     * this guarantees max number of sockets we can open
     */
    private Semaphore createPermit = new Semaphore(maxSize);

    //TTL of the connections (seconds)
    private int timeToLive;

    //Params used for resiliency of connection creation
    private ConnectionCreatorResiliencyParams resiliencyParams;

    //Internal poolReference to store connections
    private BlockingQueue<ExpirableConnection> connections;

    private ConnectionValidator<T> validator;

    private ConnectionFactory<T> connectionFactory;

    //Thread pool for executing returns of used connections.
    private ExecutorService connectionReturnerExecutor =
            Executors.newCachedThreadPool();

    private ScheduledExecutorService connectionExpireExecutor =
            Executors.newScheduledThreadPool(1);

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
     * Lock for synchronizing read and write operations to the pool
     */
    private ReentrantReadWriteLock poolLock;
    private Lock writeLock;
    private Lock readLock;


    /**
     * Create a BoundedBlockingPool. This pool has a initial size and
     * it can create new connections and grow up to a max size depending
     * on connection requests. However if additional connections are not used
     * they will expire and pool will shrink back to initial size. Whenever
     * a new connection is requested resiliencyParams are applied and it will
     * repeatedly try to create connection in exponential back-off manner. Connection requests will
     * get blocked until a connection is returned (optionally with a timeout).
     *
     * @param name              Name of the pool.
     * @param initialSize       Initial Size of the pool. At the start,
     *                          pool will create this number of connections.
     * @param maxSize           Max size the pool is allowed to grow.
     *                          This number of connections will get created at max.
     * @param timeToLive        TTL of the connections (in seconds). This is used to
     *                          invalidate additional connections created beyond initialSize
     * @param resiliencyParams  Parameters related to connection retry
     * @param validator         {ConnectionValidator} instance containing logic how to validate connections
     * @param connectionFactory {ConnectionFactory} instance containing logic how to create connections
     */
    public ExpandingBlockingPool(String name,
                                 int initialSize,
                                 int maxSize,
                                 int timeToLive,
                                 ConnectionCreatorResiliencyParams resiliencyParams,
                                 ConnectionValidator<T> validator,
                                 ConnectionFactory<T> connectionFactory) {

        super();

        log = new PoolLog(name, BoundedBlockingPool.class);

        poolLock = new ReentrantReadWriteLock();
        writeLock = poolLock.writeLock();
        readLock = poolLock.readLock();

        this.name = name;
        this.initialSize = initialSize;
        this.maxSize = maxSize;
        this.timeToLive = timeToLive;
        this.resiliencyParams = resiliencyParams;
        this.connectionFactory = connectionFactory;
        this.validator = validator;

        connections = new LinkedBlockingQueue<ExpirableConnection>(initialSize);

        initializeConnections();

        //Once every 30 sec try to cleanup expired connections
        connectionExpireExecutor.scheduleAtFixedRate(new ConnectionExpirationTask(),
                60, 30, TimeUnit.SECONDS);

        shutdownCalled = false;
    }

    public T get(long timeOut, TimeUnit unit) {
        if (!shutdownCalled) {
            ExpirableConnection expirableConnection = null;
            boolean timeUtilized = false;
            try {
                readLock.lock();

                ExpirableConnection nextConnection = connections.peek();
                if (nextConnection == null) {
                    requestNewConnectionCreate();
                }

                expirableConnection = connections.poll();
                if (expirableConnection == null) {
                    expirableConnection = connections.poll(timeOut, unit);
                    timeUtilized = true;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                readLock.unlock();
            }

            if (expirableConnection != null) {
                T connection = expirableConnection.getConnection();
                if (isValid(connection)) {
                    return connection;
                } else {
                    if (log.isDebugOn()) {
                        log.debug("Test on borrow failed for expirableConnection");
                    }
                    //TODO: can this block the call by any chance?
                    handleInvalidConnection(connection);
                }
            } else {
                if (timeUtilized) {      //we have no time to get and validate another
                    return null;
                } else {
                    get(timeOut, unit);
                }
            }

        }
        throw new IllegalStateException("Connection pool is already shutdown");
    }

    public T get() {
        return testAndGetConnection();
    }

    @Override
    protected void handleInvalidReturn(T invalidConnection) {
        handleInvalidConnection(invalidConnection);
    }

    @Override
    protected void returnToPool(T connection) {
        if (validator.isValid(connection)) {
            connectionReturnerExecutor.submit(new ExpandingBlockingPool.ConnectionReturner(connections, connection));
        }
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
     * the initial initialSize of the pool
     */
    private void initializeConnections() {
        for (int i = 0; i < initialSize; i++) {
            T connection = connectionFactory.createNew();
            connections.add(new ExpirableConnection(connection));
        }
        if (log.isDebugOn()) {
            log.debug("Connection pool initialized with" + initialSize + " connections.");
        }
    }

    /**
     * Test and get a connection. This will only return a validated connection.
     * It will recursively try all connections in the pool until it meets one.
     * Same time, it will handle failed connections. The call will get blocked
     * until it meets a valid connection to return
     *
     * @return Validated connection
     */
    private T testAndGetConnection() {
        if (!shutdownCalled) {
            ExpirableConnection expirableConnection = null;
            try {
                //TODO: we need to trigget connection creation as applicable (use requestNewConnectionCreate() but should not create namy tasks without check)
                readLock.lock();
                ExpirableConnection nextConnection = connections.peek();
                if (nextConnection == null) {
                    /*
                     * Pool has no connections. If possible
                     * place a connection creation request and see.
                     */
                    requestNewConnectionCreate();
                }
                /*
                 * Even if creation request is not placed (max already created) has to wait for a connection
                 * Even requested connection is grabbed by a different request, has to wait for a connection
                 */
                expirableConnection = connections.take();

            } catch (InterruptedException ie) {
                log.error("Thread " + Thread.currentThread().getId() + " could not obtain a expirableConnection", ie);
                Thread.currentThread().interrupt();
            } finally {
                readLock.unlock();
            }
            //TODO: if thread is interrupted what happens? Should we return null or try recursively?
            /*
             * There is no need to check if connection
             * is expired (sat too long in the pool).
             * As long as it is valid it is fine.
             */
            if (expirableConnection != null) {
                T connection = expirableConnection.getConnection();
                if (isValid(connection)) {
                    return connection;
                } else {
                    if (log.isDebugOn()) {
                        log.debug("Test on borrow failed for expirableConnection");
                    }
                    //TODO: can this block the call by any chance?
                    handleInvalidConnection(connection);
                    //recursively try to get a expirableConnection
                    return testAndGetConnection();
                }
            }
            return null;
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
        createPermit.release();
        boolean result = requestNewConnectionCreate();
        if (!result && log.isDebugOn()) {
            log.debug("Connection creation request for invalidated "
                    + "connection is not made");
        }
    }

    /**
     * Try to Request to create a new connection and add to the pool.
     * The invoking thread will not get blocked until connection creation.
     * It will return immediately.
     *
     * @return {true} if request is made. {false} if max number of connections
     * are already made at the time of invocation
     */
    private boolean requestNewConnectionCreate() {

        //check max number of connections are already created. If not, request
        boolean hasPermit = createPermit.tryAcquire();

        if (hasPermit) {
            connectionCreatorExecutor.submit(new ConnectionCreator(name, new ExpirableConnectionAdder(), connectionFactory,
                    resiliencyParams));
            if (log.isDebugOn()) {
                log.debug("A connection creation request submitted");
            }
        } else {
            if (log.isDebugOn()) {
                log.debug("Connection request is discarded as max number of connections "
                        + maxSize + " are already created.");
            }
        }

        return hasPermit;
    }

    /**
     * Clear all resources of connection pool.
     * Connections are closed. Sockets are released.
     */
    private void clearResources() {
        for (ExpirableConnection expirableConnection : connections) {
            validator.invalidate(expirableConnection.getConnection());
        }
    }


    /**
     * Wrapper class for connections to
     * keep metadata. We do not need to
     * modify expiry time as whenever we insert back
     * to the pool a new wrapper is created.
     */
    private class ExpirableConnection {

        private T connection;

        private long expiryTime;

        ExpirableConnection(T connection) {
            this.connection = connection;
            this.expiryTime = System.currentTimeMillis() + (timeToLive * 1000);
        }

        long getExpiryTime() {
            return expiryTime;
        }

        T getConnection() {
            return connection;
        }

        boolean isExpired() {
            return this.expiryTime < System.currentTimeMillis();
        }
    }

    /**
     * Represents how connection inserts are done to the pool
     */
    private class ExpirableConnectionAdder implements PoolConnectionAdder<T> {
        //TODO: do we need to acquire a write lock? Also check size and giveup
        public boolean add(T connection) {
            boolean success = connections.add(new ExpirableConnection(connection));
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
     * if the poolReference is full.
     */
    private class ConnectionReturner implements Callable<Void> {
        private BlockingQueue<ExpirableConnection> poolReference;
        private T connectionToReturn;

        public ConnectionReturner(BlockingQueue<ExpirableConnection> poolReference, T connectionToReturn) {
            this.poolReference = poolReference;
            this.connectionToReturn = connectionToReturn;
        }

        public Void call() {
            while (true) {
                try {
                    //this call will wait if there's no space available
                    poolReference.put(new ExpirableConnection(connectionToReturn));
                    break;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            return null;
        }
    }

    /**
     * Callable that will inspect connections and remove from pool
     * if they are expired. <i>This should not be run too frequently
     * as it locks the pool functionality.</i>
     * <p>
     * Expiration happens based on the time connection remained
     * in the pool. Thus at the time of insert of a connection
     * to the pool timestamp needs to be updated. So if a connection
     * is used, upon releasing it back to the pool, expiration timestamp is
     * updated and the fact that it has been used by the app is
     * captured by that.
     */
    private class ConnectionExpirationTask implements Runnable {

        public void run() {
            if (connections.size() > initialSize) {
                checkAndRemoveExpiredConnections();
            }
        }

        private void checkAndRemoveExpiredConnections() {
            try {
                writeLock.lock();
                //get a snapshot of the pool objects, sort and remove expired connections
                LinkedList<ExpirableConnection> cloneOfPool = new LinkedList<ExpirableConnection>(connections);
                getSortedList(cloneOfPool);

                for (ExpirableConnection expirableConnection : cloneOfPool) {
                    if (expirableConnection.isExpired()) {
                        boolean removed = connections.remove(expirableConnection);
                        if (removed) {
                            //TODO: we can permit another connection
                            validator.invalidate(expirableConnection.getConnection());
                        } else {
                            log.error("Expired connection is not in the pool to remove");
                        }
                    } else {
                        //as list is sorted, definitely
                        // other connections are not expired
                        break;
                    }

                    //Even expired, as long as connection is valid application can use it.
                    if (connections.size() <= initialSize) {
                        break;
                    }
                }

                //no need to maintain cloneOfPool as objects are open for GC
                //after the operation

            } finally {
                writeLock.unlock();
            }
        }

        /**
         * Sort the connection list by expiry timestamp
         *
         * @param cloneOfPool List to sort
         */
        private void getSortedList(LinkedList cloneOfPool) {
            Collections.sort(cloneOfPool, new Comparator<ExpirableConnection>() {

                public int compare(ExpirableConnection c1, ExpirableConnection c2) {
                    long t1 = c1.getExpiryTime();
                    long t2 = c2.getExpiryTime();

                    if (t1 == t2) {
                        return 0;
                    } else if (t1 > t2) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
        }
    }
}
