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
package org.axonframework.extension.multitenancy.messaging.core.annotation;

import org.axonframework.extension.multitenancy.common.TenantResolver;
import org.axonframework.extension.multitenancy.common.TenantComponentRegistry;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TenantComponentResolver}.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 */
class TenantComponentResolverTest {

    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");
    private static final TenantDescriptor TENANT_B = TenantDescriptor.tenantWithId("tenant-b");

    private TenantComponentRegistry<String> registry;
    private TenantResolver<Message> tenantResolver;

    @BeforeEach
    void setUp() {
        registry = new TenantComponentRegistry<>(
                String.class,
                tenant -> "component-for-" + tenant.tenantId()
        );
        registry.registerTenant(TENANT_A);
        registry.registerTenant(TENANT_B);
        tenantResolver = (message, tenants) -> TENANT_A;
    }

    @Nested
    class Matches {

        @Test
        void returnsTrueWhenTenantCanBeResolved() {
            // given
            TenantComponentResolver<String> resolver = new TenantComponentResolver<>(registry, tenantResolver);
            ProcessingContext context = contextWithMessage();

            // when
            boolean result = resolver.matches(context);

            // then
            assertThat(result).isTrue();
        }

        @Test
        void returnsFalseWhenNoMessageInContext() {
            // given
            TenantComponentResolver<String> resolver = new TenantComponentResolver<>(registry, tenantResolver);
            ProcessingContext context = new StubProcessingContext();

            // when
            boolean result = resolver.matches(context);

            // then
            assertThat(result).isFalse();
        }

        @Test
        void returnsFalseWhenTenantResolutionFails() {
            // given
            TenantResolver<Message> failingResolver = (message, tenants) -> {
                throw new RuntimeException("Cannot resolve tenant");
            };
            TenantComponentResolver<String> resolver = new TenantComponentResolver<>(registry, failingResolver);
            ProcessingContext context = contextWithMessage();

            // when
            boolean result = resolver.matches(context);

            // then
            assertThat(result).isFalse();
        }
    }

    @Nested
    class ResolveParameterValue {

        @Test
        void resolvesCorrectTenantScopedInstance() {
            // given
            TenantComponentResolver<String> resolver = new TenantComponentResolver<>(registry, tenantResolver);
            ProcessingContext context = contextWithMessage();

            // when
            String result = resolver.resolveParameterValue(context)
                                    .orTimeout(5, TimeUnit.SECONDS)
                                    .join();

            // then
            assertThat(result).isEqualTo("component-for-tenant-a");
        }

        @Test
        void resolvesToDifferentInstancePerTenant() {
            // given
            TenantResolver<Message> resolverForB = (message, tenants) -> TENANT_B;
            TenantComponentResolver<String> resolverA = new TenantComponentResolver<>(registry, tenantResolver);
            TenantComponentResolver<String> resolverB = new TenantComponentResolver<>(registry, resolverForB);
            ProcessingContext context = contextWithMessage();

            // when
            String resultA = resolverA.resolveParameterValue(context)
                                      .orTimeout(5, TimeUnit.SECONDS)
                                      .join();
            String resultB = resolverB.resolveParameterValue(context)
                                      .orTimeout(5, TimeUnit.SECONDS)
                                      .join();

            // then
            assertThat(resultA).isEqualTo("component-for-tenant-a");
            assertThat(resultB).isEqualTo("component-for-tenant-b");
        }

        @Test
        void returnsFailedFutureWhenNoMessageInContext() {
            // given
            TenantComponentResolver<String> resolver = new TenantComponentResolver<>(registry, tenantResolver);
            ProcessingContext context = new StubProcessingContext();

            // when
            var future = resolver.resolveParameterValue(context);

            // then
            assertThat(future).isCompletedExceptionally();
        }

        @Test
        void returnsFailedFutureWhenTenantResolutionFails() {
            // given
            TenantResolver<Message> failingResolver = (message, tenants) -> {
                throw new RuntimeException("Cannot resolve");
            };
            TenantComponentResolver<String> resolver = new TenantComponentResolver<>(registry, failingResolver);
            ProcessingContext context = contextWithMessage();

            // when
            var future = resolver.resolveParameterValue(context);

            // then
            assertThat(future).isCompletedExceptionally();
        }
    }

    private static ProcessingContext contextWithMessage() {
        Message message = new GenericMessage(new MessageType("test"), "payload");
        return StubProcessingContext.forMessage(message);
    }
}
