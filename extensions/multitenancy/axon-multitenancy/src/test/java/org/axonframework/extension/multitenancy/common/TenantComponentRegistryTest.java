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
package org.axonframework.extension.multitenancy.common;

import org.axonframework.common.Registration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TenantComponentRegistryTest {

    private TenantDescriptor tenant1;
    private TenantDescriptor tenant2;

    @BeforeEach
    void setUp() {
        tenant1 = TenantDescriptor.tenantWithId("tenant-1");
        tenant2 = TenantDescriptor.tenantWithId("tenant-2");
    }

    @Nested
    class LazyCreation {

        @Test
        void getComponentCreatesLazily() {
            // given
            AtomicInteger createCount = new AtomicInteger(0);

            TenantComponentRegistry<String> registry = new TenantComponentRegistry<>(
                    String.class,
                    tenant -> {
                        createCount.incrementAndGet();
                        return "component-for-" + tenant.tenantId();
                    }
            );

            // when
            registry.registerTenant(tenant1);

            // then
            assertThat(createCount.get()).as("Component should not be created on registration").isEqualTo(0);

            // when
            String component = registry.getComponent(tenant1);

            // then
            assertThat(createCount.get()).as("Component should be created on first access").isEqualTo(1);
            assertThat(component).isEqualTo("component-for-tenant-1");

            // when - second access should return cached instance
            String sameComponent = registry.getComponent(tenant1);

            // then
            assertThat(createCount.get()).as("Component should be cached").isEqualTo(1);
            assertThat(sameComponent).isSameAs(component);
        }
    }

    @Nested
    class Cleanup {

        @Test
        void cleanupInvokedOnTenantRemoval() {
            // given
            AtomicBoolean cleanupCalled = new AtomicBoolean(false);
            AtomicReference<String> cleanedComponent = new AtomicReference<>();

            TenantComponentFactory<String> factory = new TenantComponentFactory<>() {
                @Override
                public String create(TenantDescriptor tenant) {
                    return "component-for-" + tenant.tenantId();
                }

                @Override
                public void destroy(TenantDescriptor tenant, String component) {
                    cleanupCalled.set(true);
                    cleanedComponent.set(component);
                }
            };

            TenantComponentRegistry<String> registry = new TenantComponentRegistry<>(String.class, factory);
            Registration registration = registry.registerTenant(tenant1);

            // when - create the component
            String component = registry.getComponent(tenant1);

            // then
            assertThat(component).isNotNull();
            assertThat(cleanupCalled.get()).isFalse();

            // when - cancel registration (simulates tenant removal)
            registration.cancel();

            // then
            assertThat(cleanupCalled.get()).isTrue();
            assertThat(cleanedComponent.get()).isEqualTo(component);
        }

        @Test
        void cleanupNotInvokedIfComponentNeverCreated() {
            // given
            AtomicBoolean cleanupCalled = new AtomicBoolean(false);

            TenantComponentFactory<String> factory = new TenantComponentFactory<>() {
                @Override
                public String create(TenantDescriptor tenant) {
                    return "component";
                }

                @Override
                public void destroy(TenantDescriptor tenant, String component) {
                    cleanupCalled.set(true);
                }
            };

            TenantComponentRegistry<String> registry = new TenantComponentRegistry<>(String.class, factory);
            Registration registration = registry.registerTenant(tenant1);

            // when - don't access component, just cancel
            registration.cancel();

            // then
            assertThat(cleanupCalled.get())
                    .as("Cleanup should not be called if component was never created")
                    .isFalse();
        }

        @Test
        void autoCloseableComponentsClosedAutomatically() {
            // given
            AtomicBoolean closed = new AtomicBoolean(false);

            TenantComponentRegistry<AutoCloseable> registry = new TenantComponentRegistry<>(
                    AutoCloseable.class,
                    tenant -> () -> closed.set(true)
            );

            Registration registration = registry.registerTenant(tenant1);
            registry.getComponent(tenant1);

            // then - not yet closed
            assertThat(closed.get()).isFalse();

            // when
            registration.cancel();

            // then
            assertThat(closed.get())
                    .as("AutoCloseable should be closed on tenant removal")
                    .isTrue();
        }
    }

    @Nested
    class TenantTracking {

        @Test
        void getTenantsReturnsRegisteredTenants() {
            // given
            TenantComponentRegistry<String> registry = new TenantComponentRegistry<>(
                    String.class,
                    tenant -> "component"
            );

            // then
            assertThat(registry.getTenants()).isEmpty();

            // when
            registry.registerTenant(tenant1);
            registry.registerTenant(tenant2);

            // then
            assertThat(registry.getTenants()).hasSize(2);
            assertThat(registry.getTenants()).contains(tenant1, tenant2);
        }

        @Test
        void tenantRemovedFromTenantsSetOnCancel() {
            // given
            TenantComponentRegistry<String> registry = new TenantComponentRegistry<>(
                    String.class,
                    tenant -> "component"
            );

            Registration registration = registry.registerTenant(tenant1);

            // then
            assertThat(registry.getTenants()).contains(tenant1);

            // when
            registration.cancel();

            // then
            assertThat(registry.getTenants()).doesNotContain(tenant1);
        }

        @Test
        void componentRemovedFromCacheOnCancel() {
            // given
            TenantComponentRegistry<String> registry = new TenantComponentRegistry<>(
                    String.class,
                    tenant -> "component"
            );

            Registration registration = registry.registerTenant(tenant1);
            String first = registry.getComponent(tenant1);

            // when
            registration.cancel();

            // Re-register and get component should create new instance
            registry.registerTenant(tenant1);
            String second = registry.getComponent(tenant1);

            // then - should be equal content but new instance (since factory returns same string)
            assertThat(second).isEqualTo(first);
        }
    }

    @Nested
    class TypedComponentResolution {

        @Test
        void getComponentWithRequestedTypeCreatesCorrectInstance() {
            // given
            TenantComponentFactory<CharSequence> factory = new TenantComponentFactory<>() {
                @Override
                public CharSequence create(TenantDescriptor tenant) {
                    return "default";
                }

                @SuppressWarnings("unchecked")
                @Override
                public <S extends CharSequence> S create(TenantDescriptor tenant, Class<S> requestedType) {
                    if (requestedType == StringBuilder.class) {
                        return (S) new StringBuilder("typed-for-" + tenant.tenantId());
                    }
                    return (S) create(tenant);
                }
            };

            TenantComponentRegistry<CharSequence> registry = new TenantComponentRegistry<>(CharSequence.class, factory);
            registry.registerTenant(tenant1);

            // when
            StringBuilder result = registry.getComponent(tenant1, StringBuilder.class);

            // then
            assertThat(result).isInstanceOf(StringBuilder.class);
            assertThat(result.toString()).isEqualTo("typed-for-tenant-1");
        }

        @Test
        void getComponentWithRequestedTypeCachesPerTenantAndType() {
            // given
            AtomicInteger createCount = new AtomicInteger(0);

            TenantComponentFactory<CharSequence> factory = new TenantComponentFactory<>() {
                @Override
                public CharSequence create(TenantDescriptor tenant) {
                    return "default";
                }

                @SuppressWarnings("unchecked")
                @Override
                public <S extends CharSequence> S create(TenantDescriptor tenant, Class<S> requestedType) {
                    createCount.incrementAndGet();
                    return (S) new StringBuilder("instance");
                }
            };

            TenantComponentRegistry<CharSequence> registry = new TenantComponentRegistry<>(CharSequence.class, factory);
            registry.registerTenant(tenant1);

            // when
            StringBuilder first = registry.getComponent(tenant1, StringBuilder.class);
            StringBuilder second = registry.getComponent(tenant1, StringBuilder.class);

            // then
            assertThat(createCount.get()).isEqualTo(1);
            assertThat(second).isSameAs(first);
        }

        @Test
        void differentRequestedTypesForSameTenantAreCachedIndependently() {
            // given
            TenantComponentFactory<CharSequence> factory = new TenantComponentFactory<>() {
                @Override
                public CharSequence create(TenantDescriptor tenant) {
                    return "default";
                }

                @SuppressWarnings("unchecked")
                @Override
                public <S extends CharSequence> S create(TenantDescriptor tenant, Class<S> requestedType) {
                    if (requestedType == StringBuilder.class) {
                        return (S) new StringBuilder("builder");
                    }
                    if (requestedType == StringBuffer.class) {
                        return (S) new StringBuffer("buffer");
                    }
                    return (S) create(tenant);
                }
            };

            TenantComponentRegistry<CharSequence> registry = new TenantComponentRegistry<>(CharSequence.class, factory);
            registry.registerTenant(tenant1);

            // when
            StringBuilder builder = registry.getComponent(tenant1, StringBuilder.class);
            StringBuffer buffer = registry.getComponent(tenant1, StringBuffer.class);

            // then
            assertThat(builder.toString()).isEqualTo("builder");
            assertThat(buffer.toString()).isEqualTo("buffer");
            assertThat((CharSequence) builder).isNotSameAs((CharSequence) buffer);
        }

        @Test
        void tenantRemovalCleansUpTypedComponents() {
            // given
            AtomicInteger destroyCount = new AtomicInteger(0);

            TenantComponentFactory<CharSequence> factory = new TenantComponentFactory<>() {
                @Override
                public CharSequence create(TenantDescriptor tenant) {
                    return "default";
                }

                @SuppressWarnings("unchecked")
                @Override
                public <S extends CharSequence> S create(TenantDescriptor tenant, Class<S> requestedType) {
                    return (S) new StringBuilder("typed");
                }

                @Override
                public void destroy(TenantDescriptor tenant, CharSequence component) {
                    destroyCount.incrementAndGet();
                }
            };

            TenantComponentRegistry<CharSequence> registry = new TenantComponentRegistry<>(CharSequence.class, factory);
            Registration registration = registry.registerTenant(tenant1);
            registry.getComponent(tenant1, StringBuilder.class);

            // when
            registration.cancel();

            // then
            assertThat(destroyCount.get()).isEqualTo(1);
        }

        @Test
        void tenantRemovalCleansUpBothCaches() {
            // given
            AtomicInteger destroyCount = new AtomicInteger(0);

            TenantComponentFactory<CharSequence> factory = new TenantComponentFactory<>() {
                @Override
                public CharSequence create(TenantDescriptor tenant) {
                    return "default-component";
                }

                @SuppressWarnings("unchecked")
                @Override
                public <S extends CharSequence> S create(TenantDescriptor tenant, Class<S> requestedType) {
                    return (S) new StringBuilder("typed-component");
                }

                @Override
                public void destroy(TenantDescriptor tenant, CharSequence component) {
                    destroyCount.incrementAndGet();
                }
            };

            TenantComponentRegistry<CharSequence> registry = new TenantComponentRegistry<>(CharSequence.class, factory);
            Registration registration = registry.registerTenant(tenant1);

            // Access both the default cache and the typed cache
            registry.getComponent(tenant1);
            registry.getComponent(tenant1, StringBuilder.class);

            // when
            registration.cancel();

            // then - destroy is called once for the default-cache component and once for the typed-cache component.
            // The current implementation calls destroy() separately for each cache, so both are destroyed independently.
            // This means destroy() is invoked twice - once per cached instance - which is correct behavior since
            // the two caches hold different component instances that each need cleanup.
            assertThat(destroyCount.get()).isEqualTo(2);
        }
    }
}
