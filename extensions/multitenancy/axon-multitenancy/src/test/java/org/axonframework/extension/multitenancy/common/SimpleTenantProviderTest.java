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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SimpleTenantProviderTest {

    private SimpleTenantProvider provider;
    private TenantDescriptor tenant1;
    private TenantDescriptor tenant2;

    @BeforeEach
    void setUp() {
        provider = new SimpleTenantProvider();
        tenant1 = TenantDescriptor.tenantWithId("tenant-1");
        tenant2 = TenantDescriptor.tenantWithId("tenant-2");
    }

    @Nested
    class AddTenant {

        @Test
        void addTenantReturnsTrueWhenTenantIsNew() {
            // when
            boolean result = provider.addTenant(tenant1);

            // then
            assertThat(result).isTrue();
            assertThat(provider.getTenants()).hasSize(1);
            assertThat(provider.hasTenant(tenant1)).isTrue();
        }

        @Test
        void addTenantReturnsFalseWhenTenantAlreadyExists() {
            // given
            provider.addTenant(tenant1);

            // when
            boolean result = provider.addTenant(tenant1);

            // then
            assertThat(result).isFalse();
            assertThat(provider.getTenants()).hasSize(1);
        }

        @Test
        void addTenantNotifiesSubscribers() {
            // given
            MultiTenantAwareComponent subscriber = mock(MultiTenantAwareComponent.class);
            provider.subscribe(subscriber);

            // when
            provider.addTenant(tenant1);

            // then
            verify(subscriber).registerAndStartTenant(tenant1);
        }

        @Test
        void addTenantsAddsMultipleTenants() {
            // when
            provider.addTenants(List.of(tenant1, tenant2));

            // then
            assertThat(provider.getTenants()).hasSize(2);
            assertThat(provider.hasTenant(tenant1)).isTrue();
            assertThat(provider.hasTenant(tenant2)).isTrue();
        }
    }

    @Nested
    class RemoveTenant {

        @Test
        void removeTenantReturnsTrueWhenTenantExists() {
            // given
            provider.addTenant(tenant1);

            // when / then
            assertThat(provider.removeTenant(tenant1)).isTrue();
            assertThat(provider.hasTenant(tenant1)).isFalse();
        }

        @Test
        void removeTenantReturnsFalseWhenTenantDoesNotExist() {
            // when / then
            assertThat(provider.removeTenant(tenant1)).isFalse();
        }

        @Test
        void removeTenantByIdWorks() {
            // given
            provider.addTenant(tenant1);

            // when / then
            assertThat(provider.removeTenant("tenant-1")).isTrue();
            assertThat(provider.hasTenant("tenant-1")).isFalse();
        }

        @Test
        void removeTenantTriggersCleanupRegistrations() {
            // given
            AtomicInteger cleanupCount = new AtomicInteger(0);

            MultiTenantAwareComponent subscriber = new MultiTenantAwareComponent() {
                @Override
                public Registration registerTenant(TenantDescriptor tenantDescriptor) {
                    return () -> {
                        cleanupCount.incrementAndGet();
                        return true;
                    };
                }

                @Override
                public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
                    return registerTenant(tenantDescriptor);
                }
            };

            provider.subscribe(subscriber);
            provider.addTenant(tenant1);

            // then
            assertThat(cleanupCount.get()).isEqualTo(0);

            // when
            provider.removeTenant(tenant1);

            // then
            assertThat(cleanupCount.get()).isEqualTo(1);
        }

        @Test
        void removeTenantTriggersCleanupForAllSubscribers() {
            // given
            AtomicInteger cleanupCount = new AtomicInteger(0);

            MultiTenantAwareComponent subscriber1 = createCleanupCountingSubscriber(cleanupCount);
            MultiTenantAwareComponent subscriber2 = createCleanupCountingSubscriber(cleanupCount);

            provider.subscribe(subscriber1);
            provider.subscribe(subscriber2);
            provider.addTenant(tenant1);

            // then
            assertThat(cleanupCount.get()).isEqualTo(0);

            // when
            provider.removeTenant(tenant1);

            // then
            assertThat(cleanupCount.get()).isEqualTo(2);
        }
    }

    @Nested
    class Subscribe {

        @Test
        void subscribeRegistersExistingTenants() {
            // given
            provider.addTenant(tenant1);
            provider.addTenant(tenant2);

            // when
            MultiTenantAwareComponent subscriber = mock(MultiTenantAwareComponent.class);
            provider.subscribe(subscriber);

            // then
            verify(subscriber).registerTenant(tenant1);
            verify(subscriber).registerTenant(tenant2);
        }

        @Test
        void unsubscribeStopsNotifications() {
            // given
            MultiTenantAwareComponent subscriber = mock(MultiTenantAwareComponent.class);
            Registration registration = provider.subscribe(subscriber);

            // when
            registration.cancel();
            provider.addTenant(tenant1);

            // then
            verify(subscriber, never()).registerAndStartTenant(tenant1);
        }

        @Test
        void subscribeAfterAddTenantStillGetsCleanupOnRemove() {
            // given
            AtomicInteger cleanupCount = new AtomicInteger(0);

            // Add tenant first
            provider.addTenant(tenant1);

            // Subscribe after
            MultiTenantAwareComponent subscriber = createCleanupCountingSubscriber(cleanupCount);
            provider.subscribe(subscriber);

            // when - cleanup should still be triggered
            provider.removeTenant(tenant1);

            // then
            assertThat(cleanupCount.get()).isEqualTo(1);
        }
    }

    @Nested
    class Construction {

        @Test
        void constructorWithInitialTenants() {
            // when
            SimpleTenantProvider providerWithTenants = new SimpleTenantProvider(List.of(tenant1, tenant2));

            // then
            assertThat(providerWithTenants.getTenants()).hasSize(2);
            assertThat(providerWithTenants.hasTenant(tenant1)).isTrue();
            assertThat(providerWithTenants.hasTenant(tenant2)).isTrue();
        }
    }

    @Nested
    class ThreadSafety {

        @Test
        void isThreadSafe() throws InterruptedException {
            // given
            AtomicInteger addCount = new AtomicInteger(0);
            MultiTenantAwareComponent subscriber = mock(MultiTenantAwareComponent.class);
            provider.subscribe(subscriber);

            Thread[] threads = new Thread[10];
            for (int i = 0; i < 10; i++) {
                int tenantNum = i;
                threads[i] = new Thread(() -> {
                    TenantDescriptor tenant = TenantDescriptor.tenantWithId("tenant-" + tenantNum);
                    if (provider.addTenant(tenant)) {
                        addCount.incrementAndGet();
                    }
                });
            }

            // when
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }

            // then
            assertThat(addCount.get()).isEqualTo(10);
            assertThat(provider.getTenants()).hasSize(10);
        }
    }

    private MultiTenantAwareComponent createCleanupCountingSubscriber(AtomicInteger cleanupCount) {
        return new MultiTenantAwareComponent() {
            @Override
            public Registration registerTenant(TenantDescriptor tenantDescriptor) {
                return () -> {
                    cleanupCount.incrementAndGet();
                    return true;
                };
            }

            @Override
            public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
                return registerTenant(tenantDescriptor);
            }
        };
    }
}
