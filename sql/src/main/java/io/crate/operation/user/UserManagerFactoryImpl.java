package io.crate.operation.user;

import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.DropUserAnalyzedStatement;
import io.crate.metadata.sys.SysSchemaInfo;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.concurrent.CompletableFuture;

public class UserManagerFactoryImpl implements UserManagerFactory {

    @Override
    public UserManager create(ClusterService clusterService, SysSchemaInfo sysSchemaInfo) {

        return new UserManager() {
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
        };
    }
}
