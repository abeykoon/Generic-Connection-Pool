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
 * Represents the mechanism how connection is added to the pool.
 * Different pool implementation may require to add metadata,
 * acquire locks before inserting connection
 *
 */
public interface PoolConnectionAdder<T> {

    /**
     * Add connection to the pool
     *
     * @param connection Connection to add
     * @return {true} if insert is done and successful
     */
    boolean add(T connection);
}
