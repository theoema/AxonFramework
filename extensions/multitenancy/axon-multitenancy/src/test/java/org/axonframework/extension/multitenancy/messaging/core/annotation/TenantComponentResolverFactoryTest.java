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

import org.axonframework.extension.multitenancy.common.TargetTenantResolver;
import org.axonframework.extension.multitenancy.common.TenantComponentRegistry;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TenantComponentResolverFactory}.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 */
class TenantComponentResolverFactoryTest {

    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");

    private TargetTenantResolver<Message> tenantResolver;
    private TenantComponentResolverFactory factory;

    @BeforeEach
    void setUp() {
        tenantResolver = (message, tenants) -> TENANT_A;
        factory = new TenantComponentResolverFactory(tenantResolver);
    }

    @Nested
    class RegisterComponent {

        @Test
        void createsRegistryAndReturnsIt() {
            // when
            TenantComponentRegistry<StringBuilder> registry = factory.registerComponent(
                    StringBuilder.class, tenant -> new StringBuilder("for-" + tenant.tenantId())
            );

            // then
            assertThat(registry).isNotNull();
            assertThat(registry.getComponentType()).isEqualTo(StringBuilder.class);
        }

        @Test
        void registeredComponentAppearsInRegistries() {
            // when
            factory.registerComponent(StringBuilder.class, tenant -> new StringBuilder());

            // then
            assertThat(factory.getRegistries()).containsKey(StringBuilder.class);
        }

        @Test
        void multipleComponentTypesCanBeRegistered() {
            // when
            factory.registerComponent(StringBuilder.class, tenant -> new StringBuilder());
            factory.registerComponent(Runnable.class, tenant -> () -> {});

            // then
            assertThat(factory.getRegistries()).hasSize(2);
            assertThat(factory.getRegistries()).containsKey(StringBuilder.class);
            assertThat(factory.getRegistries()).containsKey(Runnable.class);
        }
    }

    @Nested
    class CreateInstance {

        @Test
        void returnsResolverForRegisteredParameterType() throws Exception {
            // given
            factory.registerComponent(StringBuilder.class, tenant -> new StringBuilder());
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            Parameter[] parameters = method.getParameters();

            // when
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            // then
            assertThat(resolver).isNotNull();
            assertThat(resolver).isInstanceOf(TenantComponentResolver.class);
        }

        @Test
        void returnsNullForUnregisteredParameterType() throws Exception {
            // given - no component registered for StringBuilder
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            Parameter[] parameters = method.getParameters();

            // when
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            // then
            assertThat(resolver).isNull();
        }

        @Test
        void returnsNullForNonMatchingParameterType() throws Exception {
            // given
            factory.registerComponent(Runnable.class, tenant -> () -> {});
            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            Parameter[] parameters = method.getParameters();

            // when - StringBuilder parameter but Runnable is registered
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            // then
            assertThat(resolver).isNull();
        }
    }

    @Nested
    class ResolveParameterValue {

        @Test
        void resolvesToTenantScopedComponent() throws Exception {
            // given
            factory.registerComponent(StringBuilder.class,
                    tenant -> new StringBuilder("value-for-" + tenant.tenantId()));
            TenantComponentRegistry<StringBuilder> registry =
                    (TenantComponentRegistry<StringBuilder>) factory.getRegistries().get(StringBuilder.class);
            registry.registerTenant(TENANT_A);

            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            Parameter[] parameters = method.getParameters();
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            Message message = new GenericMessage(new MessageType("test"), "payload");
            ProcessingContext context = StubProcessingContext.forMessage(message);

            // when
            Object result = resolver.resolveParameterValue(context).orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();

            // then
            assertThat(result).isInstanceOf(StringBuilder.class);
            assertThat(result.toString()).isEqualTo("value-for-tenant-a");
        }
    }

    @Nested
    class SubtypeResolution {

        @Test
        void returnsResolverForSubtypeOfRegisteredComponentType() throws Exception {
            // given
            factory.registerComponent(CharSequence.class, tenant -> "value-for-" + tenant.tenantId());
            Method method = SubtypeHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            Parameter[] parameters = method.getParameters();

            // when
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            // then
            assertThat(resolver).isNotNull();
            assertThat(resolver).isInstanceOf(TenantComponentResolver.class);
        }

        @Test
        void subtypeResolverPassesRequestedTypeToRegistry() throws Exception {
            // given - factory uses create(tenant, requestedType) to produce the requested subtype
            factory.registerComponent(CharSequence.class, new org.axonframework.extension.multitenancy.common.TenantComponentFactory<>() {
                @Override
                public CharSequence create(org.axonframework.extension.multitenancy.common.TenantDescriptor tenant) {
                    return "default";
                }

                @SuppressWarnings("unchecked")
                @Override
                public <S extends CharSequence> S create(org.axonframework.extension.multitenancy.common.TenantDescriptor tenant, Class<S> requestedType) {
                    if (requestedType == StringBuilder.class) {
                        return (S) new StringBuilder("subtype-for-" + tenant.tenantId());
                    }
                    return (S) create(tenant);
                }
            });
            TenantComponentRegistry<CharSequence> registry =
                    (TenantComponentRegistry<CharSequence>) factory.getRegistries().get(CharSequence.class);
            registry.registerTenant(TENANT_A);

            Method method = SubtypeHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            Parameter[] parameters = method.getParameters();
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            Message message = new GenericMessage(new MessageType("test"), "payload");
            ProcessingContext context = StubProcessingContext.forMessage(message);

            // when
            Object result = resolver.resolveParameterValue(context).orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();

            // then
            assertThat(result).isInstanceOf(StringBuilder.class);
            assertThat(result.toString()).isEqualTo("subtype-for-tenant-a");
        }

        @Test
        void exactMatchTakesPriorityOverSubtypeMatch() throws Exception {
            // given - register both CharSequence (supertype) and StringBuilder (exact match)
            factory.registerComponent(CharSequence.class, tenant -> "charsequence-value");
            factory.registerComponent(StringBuilder.class, tenant -> new StringBuilder("exact-match"));

            Method method = SubtypeHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            Parameter[] parameters = method.getParameters();

            // when
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            // then - should use exact match registry, which creates a TenantComponentResolver without requestedType
            assertThat(resolver).isNotNull();

            // Verify behavior: register tenants and resolve
            TenantComponentRegistry<StringBuilder> registry =
                    (TenantComponentRegistry<StringBuilder>) factory.getRegistries().get(StringBuilder.class);
            registry.registerTenant(TENANT_A);

            Message message = new GenericMessage(new MessageType("test"), "payload");
            ProcessingContext context = StubProcessingContext.forMessage(message);
            Object result = resolver.resolveParameterValue(context).orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();

            assertThat(result).isInstanceOf(StringBuilder.class);
            assertThat(result.toString()).isEqualTo("exact-match");
        }

        @Test
        void returnsNullWhenNoRegistryMatchesParameterType() throws Exception {
            // given - Runnable is not assignable from StringBuilder
            factory.registerComponent(Runnable.class, tenant -> () -> {});
            Method method = SubtypeHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            Parameter[] parameters = method.getParameters();

            // when
            ParameterResolver<?> resolver = factory.createInstance(method, parameters, 1);

            // then
            assertThat(resolver).isNull();
        }
    }

    /**
     * Dummy handler class used to obtain Method/Parameter reflective objects.
     */
    @SuppressWarnings("unused")
    static class SampleHandler {

        void handle(String event, StringBuilder component) {
        }
    }

    /**
     * Handler class with a supertype parameter, used for subtype resolution tests.
     */
    @SuppressWarnings("unused")
    static class SubtypeHandler {

        void handle(String event, StringBuilder component) {
        }
    }
}
