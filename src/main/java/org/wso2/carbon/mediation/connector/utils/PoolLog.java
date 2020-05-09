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

package org.wso2.carbon.mediation.connector.utils;

import org.apache.commons.logging.LogFactory;

/**
 * Helper class for logging
 */
public class PoolLog {

    private org.apache.commons.logging.Log log;
    private String poolName;

    /**
     * Constructor log PoolLog
     *
     * @param poolName name of the pool
     * @param clazz    Class to refer when logging
     */
    public PoolLog(String poolName, Class clazz) {
        this.log = LogFactory.getLog(clazz);
        this.poolName = poolName;
    }

    public boolean isDebugOn() {
        //return log.isDebugEnabled();
        return true;
    }

    public boolean isTraceOn() {
        return log.isTraceEnabled();
    }

    public void error(Object o) {
        //log.error("[" + poolName + "] " + o);
        System.out.println("[" + poolName + "] " + o);
    }

    public void error(Object o, Throwable throwable) {
        //log.error("[" + poolName + "] " + o, throwable);
        System.out.println("[" + poolName + "] " + o + throwable.getMessage());
    }

    public void warn(Object o) {
        //log.warn("[" + poolName + "] " + o);
        System.out.println("[" + poolName + "] " + o);
    }

    public void warn(Object o, Throwable throwable) {
        //log.warn("[" + poolName + "] " + o, throwable);
        System.out.println("[" + poolName + "] " + o + throwable.getMessage());
    }

    public void info(Object o) {
        //log.info("[" + poolName + "] " + o);
        System.out.println("[" + poolName + "] " + o);
    }

    public void info(Object o, Throwable throwable) {
        //log.info("[" + poolName + "] " + o, throwable);
        System.out.println("[" + poolName + "] " + o + throwable.getMessage());
    }

    public void debug(Object o) {
        //log.debug("[" + poolName + "] " + o);
        System.out.println("[" + poolName + "] " + o);
    }

    public void trace(Object o) {
        //log.trace("[" + poolName + "] " + o);
        System.out.println("[" + poolName + "] " + o);
    }
}
