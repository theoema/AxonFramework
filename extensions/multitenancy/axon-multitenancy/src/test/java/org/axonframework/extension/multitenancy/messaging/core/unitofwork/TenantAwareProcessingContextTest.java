/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.axonframework.extension.multitenancy.messaging.core.unitofwork;

import org.axonframework.extension.multitenancy.common.TargetTenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.extension.multitenancy.messaging.core.annotation.TenantComponentResolverFactory;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link TenantAwareProcessingContext}.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 */
class TenantAwareProcessingContextTest {

    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");

    private TargetTenantResolver<Message> tenantResolver;
    private TenantComponentResolverFactory resolverFactory;
    private StubProcessingContext delegate;
    private TenantAwareProcessingContext tenantAwareContext;

    @BeforeEach
    void setUp() {
        tenantResolver = (message, tenants) -> TENANT_A;
        resolverFactory = new TenantComponentResolverFactory(tenantResolver);
        delegate = new StubProcessingContext();
        tenantAwareContext = new TenantAwareProcessingContext(delegate, resolverFactory, TENANT_A);
    }

    @Nested
    class Component {

        @Test
        void returnsTenantScopedInstanceWhenTypeIsRegistered() {
            // given
            resolverFactory.registerComponent(StringBuilder.class,
                    tenant -> new StringBuilder("for-" + tenant.tenantId()));
            resolverFactory.getRegistries().get(StringBuilder.class).registerTenant(TENANT_A);

            // when
            StringBuilder result = tenantAwareContext.component(StringBuilder.class);

            // then
            assertThat(result.toString()).isEqualTo("for-tenant-a");
        }

        @Test
        void returnsTenantScopedInstanceWhenTypeIsRegisteredWithName() {
            // given
            resolverFactory.registerComponent(StringBuilder.class,
                    tenant -> new StringBuilder("named-for-" + tenant.tenantId()));
            resolverFactory.getRegistries().get(StringBuilder.class).registerTenant(TENANT_A);

            // when
            StringBuilder result = tenantAwareContext.component(StringBuilder.class, "someName");

            // then
            assertThat(result.toString()).isEqualTo("named-for-tenant-a");
        }

        @Test
        void delegatesToWrappedContextWhenTypeNotRegistered() {
            // given - no components registered in the factory
            StubProcessingContext spiedDelegate = spy(new StubProcessingContext());
            TenantAwareProcessingContext context =
                    new TenantAwareProcessingContext(spiedDelegate, resolverFactory, TENANT_A);

            // when / then - delegates to the wrapped context, which will throw ComponentNotFoundException
            try {
                context.component(Runnable.class);
            } catch (Exception ignored) {
                // expected, StubProcessingContext delegates to EmptyApplicationContext which throws
            }

            // then
            verify(spiedDelegate).component(Runnable.class);
        }

        @Test
        void delegatesToWrappedContextWithNameWhenTypeNotRegistered() {
            // given
            StubProcessingContext spiedDelegate = spy(new StubProcessingContext());
            TenantAwareProcessingContext context =
                    new TenantAwareProcessingContext(spiedDelegate, resolverFactory, TENANT_A);

            // when / then
            try {
                context.component(Runnable.class, "myName");
            } catch (Exception ignored) {
                // expected
            }

            // then
            verify(spiedDelegate).component(Runnable.class, "myName");
        }
    }

    @Nested
    class DelegatingMethods {

        @Test
        void resourceOperationsDelegate() {
            // given
            Context.ResourceKey<String> key = Context.ResourceKey.withLabel("test-key");

            // when
            tenantAwareContext.putResource(key, "value");

            // then - resource should be in delegate
            assertThat(delegate.getResource(key)).isEqualTo("value");
            assertThat(tenantAwareContext.getResource(key)).isEqualTo("value");
            assertThat(tenantAwareContext.containsResource(key)).isTrue();
        }

        @Test
        void removeResourceDelegates() {
            // given
            Context.ResourceKey<String> key = Context.ResourceKey.withLabel("test-key");
            delegate.putResource(key, "value");

            // when
            String removed = tenantAwareContext.removeResource(key);

            // then
            assertThat(removed).isEqualTo("value");
            assertThat(delegate.containsResource(key)).isFalse();
        }

        @Test
        void phaseQueryMethodsDelegate() {
            // then - StubProcessingContext returns false for all phase queries
            assertThat(tenantAwareContext.isStarted()).isEqualTo(delegate.isStarted());
            assertThat(tenantAwareContext.isError()).isEqualTo(delegate.isError());
            assertThat(tenantAwareContext.isCommitted()).isEqualTo(delegate.isCommitted());
            assertThat(tenantAwareContext.isCompleted()).isEqualTo(delegate.isCompleted());
        }
    }
}
