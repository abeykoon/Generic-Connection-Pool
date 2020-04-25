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

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Connection creator with resilience.
 * It will try to create the connection
 * in an exponential back-off manner.
 * Algorithm :
 * <code>wait_interval = (base * multiplier^n) +/- (random interval)</code>
 *
 * @param <T>
 */
public class ConnectionCreator<T> implements Callable<Void> {

    private PoolLog log;

    //base interval
    private int INITIAL_INTERVAL;

    //interval will increase by this factor for each try
    private float MULTIPLIER;

    //max number of times to retry
    private int MAX_RETRIES;

    //max interval between two retries
    private int MAX_INTERVAL;

    //current iteration
    private int CURRENT_RETRY;

    //impl of how connection is added to pool
    private PoolConnectionAdder connectionAdder;
    private ConnectionFactory<T> connectionFactory;

    public ConnectionCreator(String poolName,
                             PoolConnectionAdder connectionAdder,
                             ConnectionFactory<T> connectionFactory,
                             Pool.ConnectionCreatorResiliencyParams params) {

        this.log = new PoolLog(poolName, ConnectionCreator.class);
        this.CURRENT_RETRY = 0;
        this.INITIAL_INTERVAL = params.initialInterval;
        this.MULTIPLIER = params.multiplier;
        this.MAX_RETRIES = params.maxRetries;
        this.MAX_INTERVAL = params.maxInterval;
        this.connectionAdder = connectionAdder;
        this.connectionFactory = connectionFactory;
    }

    public Void call() throws Exception {

        while (MAX_RETRIES == -1 || CURRENT_RETRY <= MAX_RETRIES) {

            CURRENT_RETRY = CURRENT_RETRY + 1;

            try {
                //TODO: here we assume pool will not exceed its size when insert. Otherwise it may lead endless loop
                boolean added = connectionAdder.add((connectionFactory.createNew()));
                if (added) {
                    break;
                } else {
                    if ((MAX_RETRIES != -1) && CURRENT_RETRY > MAX_RETRIES) {
                        log.error("Could not create connection after max retries " + MAX_RETRIES
                                + ". Giving up.");
                    }
                }
            } catch (Throwable e) {     //loop should not break upon any type of exception
                log.error("Error while creating connection. Retry Iteration = "
                        + CURRENT_RETRY + ". Max number of retries = " + MAX_RETRIES);
            }

            int finalWaitTime = getTimeToWait();

            if (log.isDebugOn()) {
                log.debug("Next retry to create connection in " + (finalWaitTime) + "seconds.");
            }

            TimeUnit.SECONDS.sleep(finalWaitTime);
        }

        return null;
    }

    /**
     * Calculate time to wait according to backoff algorithm
     */
    private int getTimeToWait() {
        float primaryWaitInterval = INITIAL_INTERVAL + (INITIAL_INTERVAL * MULTIPLIER * CURRENT_RETRY);
        boolean isToAddRandomTime = getRandomBoolean();
        float randomTime = getRandomNumberBetweenTwoNumbers(INITIAL_INTERVAL, 0);
        float finalWaitTime;
        if (isToAddRandomTime) {
            finalWaitTime = primaryWaitInterval + randomTime;
        } else {
            finalWaitTime = primaryWaitInterval - randomTime;
        }

        //we do not allow it to exceed max wait interval
        if (finalWaitTime > MAX_INTERVAL) {
            finalWaitTime = MAX_INTERVAL;
        }

        if (log.isDebugOn()) {
            log.debug("Retry time calculation detail: [primaryWaitInterval=" + primaryWaitInterval + "]"
                    + " , [randomTime=" + randomTime + "]"
                    + " , [isToAddRandomTime=" + isToAddRandomTime + "]"
                    + " . [finalWaitTime=" + finalWaitTime + "]");
        }

        return Math.round(finalWaitTime);
    }


    private boolean getRandomBoolean() {
        Random random = new Random();
        return random.nextBoolean();
    }

    private int getRandomNumberBetweenTwoNumbers(int high, int low) {
        Random r = new Random();
        return r.nextInt(high - low) + low;
    }
}