/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.user;

import io.crate.metadata.sys.SysSchemaInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

@Singleton
public class UserManagerProvider implements Provider<UserManager> {

    private final UserManager userManager;

    @Inject
    public UserManagerProvider(ClusterService clusterService,
                               SysSchemaInfo sysSchemaInfo) {
        Iterator<UserManager> userManagerIterator = ServiceLoader.load(UserManager.class).iterator();
        UserManager userManager = null;
        while (userManagerIterator.hasNext()) {
            if (userManager != null) {
                throw new ServiceConfigurationError("UserManager implemented twice");
            }
            userManager = userManagerIterator.next();
        }
        if (userManager != null) {
            userManager.initialize(clusterService, sysSchemaInfo);
        }
        this.userManager = userManager;
    }


    @Override
    public UserManager get() {
        return userManager;
    }
}
