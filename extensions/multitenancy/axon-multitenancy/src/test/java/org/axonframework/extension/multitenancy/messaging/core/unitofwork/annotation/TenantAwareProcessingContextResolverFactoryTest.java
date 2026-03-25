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
package org.axonframework.extension.multitenancy.messaging.core.unitofwork.annotation;

import org.axonframework.extension.multitenancy.common.TenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.extension.multitenancy.messaging.core.annotation.TenantComponentResolverFactory;
import org.axonframework.extension.multitenancy.messaging.core.unitofwork.TenantAwareProcessingContext;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TenantAwareProcessingContextResolverFactory}.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 */
class TenantAwareProcessingContextResolverFactoryTest {

    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");

    private TenantResolver<Message> tenantResolver;
    private TenantComponentResolverFactory componentFactory;
    private TenantAwareProcessingContextResolverFactory factory;

    @BeforeEach
    void setUp() {
        tenantResolver = (message, tenants) -> TENANT_A;
        componentFactory = new TenantComponentResolverFactory(tenantResolver);
        factory = new TenantAwareProcessingContextResolverFactory(componentFactory, tenantResolver);
    }

    @Nested
    class CreateInstance {

        @Test
        void returnsResolverForProcessingContextParameter() throws Exception {
            // given
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, ProcessingContext.class);
            Parameter[] parameters = method.getParameters();

            // when
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            // then
            assertThat(resolver).isNotNull();
            assertThat(resolver).isInstanceOf(TenantAwareProcessingContextResolver.class);
        }

        @Test
        void returnsNullForNonProcessingContextParameter() throws Exception {
            // given
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, ProcessingContext.class);
            Parameter[] parameters = method.getParameters();

            // when - index 0 is String, not ProcessingContext
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 0);

            // then
            assertThat(resolver).isNull();
        }
    }

    @Nested
    class ResolveParameterValue {

        @Test
        void resolvesToTenantAwareProcessingContextWhenComponentsRegistered() throws Exception {
            // given
            componentFactory.registerComponent(StringBuilder.class,
                    tenant -> new StringBuilder("for-" + tenant.tenantId()));
            componentFactory.getRegistries().get(StringBuilder.class).registerTenant(TENANT_A);

            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, ProcessingContext.class);
            Parameter[] parameters = method.getParameters();
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            Message message = new GenericMessage(new MessageType("test"), "payload");
            ProcessingContext context = StubProcessingContext.forMessage(message);

            // when
            Object result = resolver.resolveParameterValue(context)
                                    .orTimeout(5, TimeUnit.SECONDS)
                                    .join();

            // then
            assertThat(result).isInstanceOf(TenantAwareProcessingContext.class);
        }

        @Test
        void returnsOriginalContextWhenNoComponentsRegistered() throws Exception {
            // given - no components registered
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, ProcessingContext.class);
            Parameter[] parameters = method.getParameters();
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            Message message = new GenericMessage(new MessageType("test"), "payload");
            ProcessingContext context = StubProcessingContext.forMessage(message);

            // when
            Object result = resolver.resolveParameterValue(context)
                                    .orTimeout(5, TimeUnit.SECONDS)
                                    .join();

            // then - should return the original context, not a wrapped one
            assertThat(result).isNotInstanceOf(TenantAwareProcessingContext.class);
            assertThat(result).isSameAs(context);
        }

        @Test
        void returnsOriginalContextWhenNoMessageInContext() throws Exception {
            // given
            componentFactory.registerComponent(StringBuilder.class,
                    tenant -> new StringBuilder("for-" + tenant.tenantId()));
            componentFactory.getRegistries().get(StringBuilder.class).registerTenant(TENANT_A);

            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, ProcessingContext.class);
            Parameter[] parameters = method.getParameters();
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            ProcessingContext context = new StubProcessingContext();

            // when
            Object result = resolver.resolveParameterValue(context)
                                    .orTimeout(5, TimeUnit.SECONDS)
                                    .join();

            // then - falls back to original context
            assertThat(result).isSameAs(context);
        }

        @Test
        void resolverAlwaysMatches() throws Exception {
            // given
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, ProcessingContext.class);
            Parameter[] parameters = method.getParameters();
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            ProcessingContext context = new StubProcessingContext();

            // when
            boolean matches = resolver.matches(context);

            // then
            assertThat(matches).isTrue();
        }
    }

    /**
     * Dummy handler class used to obtain Method/Parameter reflective objects.
     */
    @SuppressWarnings("unused")
    static class SampleHandler {

        void handle(String event, ProcessingContext context) {
        }
    }
}
