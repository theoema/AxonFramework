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
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TenantDescriptorParameterResolverFactory}.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 */
class TenantDescriptorParameterResolverFactoryTest {

    private static final TenantDescriptor TENANT_A =
            new TenantDescriptor("tenant-a", Map.of("region", "eu-west-1"));

    private TenantResolver<Message> tenantResolver;
    private TenantDescriptorParameterResolverFactory factory;

    @BeforeEach
    void setUp() {
        tenantResolver = (message, tenants) -> TENANT_A;
        factory = new TenantDescriptorParameterResolverFactory(tenantResolver);
    }

    @Nested
    class CreateInstance {

        @Test
        void returnsResolverForTenantDescriptorParameter() throws Exception {
            // given
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, TenantDescriptor.class);
            Parameter[] parameters = method.getParameters();

            // when
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            // then
            assertThat(resolver).isNotNull();
        }

        @Test
        void returnsNullForNonTenantDescriptorParameter() throws Exception {
            // given
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, TenantDescriptor.class);
            Parameter[] parameters = method.getParameters();

            // when
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 0);

            // then
            assertThat(resolver).isNull();
        }
    }

    @Nested
    class ResolveParameterValue {

        @Test
        void resolvesToTenantDescriptorFromMessage() throws Exception {
            // given
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, TenantDescriptor.class);
            Parameter[] parameters = method.getParameters();
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            Message message = new GenericMessage(new MessageType("test"), "payload");
            ProcessingContext context = StubProcessingContext.forMessage(message);

            // when
            Object result = resolver.resolveParameterValue(context)
                                    .orTimeout(5, TimeUnit.SECONDS)
                                    .join();

            // then
            assertThat(result).isInstanceOf(TenantDescriptor.class);
            TenantDescriptor resolved = (TenantDescriptor) result;
            assertThat(resolved.tenantId()).isEqualTo("tenant-a");
            assertThat(resolved.properties()).containsEntry("region", "eu-west-1");
        }

        @Test
        void matchesReturnsTrueWhenMessagePresent() throws Exception {
            // given
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, TenantDescriptor.class);
            Parameter[] parameters = method.getParameters();
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            Message message = new GenericMessage(new MessageType("test"), "payload");
            ProcessingContext context = StubProcessingContext.forMessage(message);

            // when / then
            assertThat(resolver.matches(context)).isTrue();
        }

        @Test
        void matchesReturnsFalseWhenNoMessageInContext() throws Exception {
            // given
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, TenantDescriptor.class);
            Parameter[] parameters = method.getParameters();
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            ProcessingContext context = StubProcessingContext.withComponents(cr -> {});

            // when / then
            assertThat(resolver.matches(context)).isFalse();
        }
    }

    @SuppressWarnings("unused")
    static class SampleHandler {

        void handle(String event, TenantDescriptor tenant) {
        }
    }
}
