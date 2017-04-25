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

import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.DropUserAnalyzedStatement;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.core.Is.is;

public class UserManagementTest extends CrateUnitTest {

    private class TestUserManager implements UserManager {

        private CompletableFuture<Long> createResult() {
            CompletableFuture<Long> result = new CompletableFuture<>();
            result.complete(1L);
            return result;
        }

        @Override
        public CompletableFuture<Long> createUser(CreateUserAnalyzedStatement analysis) {
            return createResult();
        }

        @Override
        public CompletableFuture<Long> dropUser(DropUserAnalyzedStatement analysis) {
            return createResult();
        }
    }

    private class TestProvider extends UserManagerProvider {

        private final UserManager manager;

        private TestProvider(UserManager manager) {
            this.manager = manager;
        }

        @Override
        public UserManager get() {
            return manager;
        }
    }

    DDLStatementDispatcher ddlDispatcherCommunityEdition = new DDLStatementDispatcher(
            null, null, null, null, null, null,
            new TestProvider(null),
            null, null, null
        );

    DDLStatementDispatcher ddlDispatcherEnterpriseEdition = new DDLStatementDispatcher(
        null, null, null, null, null, null,
        new TestProvider(new TestUserManager()),
        null, null, null
    );

    @Test
    public void testCreateFunctionNoUserManager() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("CREATE USER is only supported in enterprise version");
        ddlDispatcherCommunityEdition.dispatch(new CreateUserAnalyzedStatement("root"), null);
    }

    @Test
    public void testDropFunctionNoUserManager() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("DROP USER is only supported in enterprise version");
        ddlDispatcherCommunityEdition.dispatch(new DropUserAnalyzedStatement("root"), null);
    }

    @Test
    public void testCreateFunctionWithUserManager() throws Exception {
        CompletableFuture<Long> res = ddlDispatcherEnterpriseEdition.dispatch(new CreateUserAnalyzedStatement("root"), null);
        assertThat(res.get(), is(1L));
    }

    @Test
    public void testDropFunctionWithUserManager() throws Exception {
        CompletableFuture<Long> res = ddlDispatcherEnterpriseEdition.dispatch(new DropUserAnalyzedStatement("root"), null);
        assertThat(res.get(), is(1L));
    }
}
