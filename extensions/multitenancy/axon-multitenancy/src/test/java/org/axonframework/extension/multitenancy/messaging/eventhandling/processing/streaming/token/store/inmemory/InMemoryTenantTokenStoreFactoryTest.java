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
package org.axonframework.extension.multitenancy.messaging.eventhandling.processing.streaming.token.store.inmemory;

import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link InMemoryTenantTokenStoreFactory}.
 *
 * @author Stefan Dragisic
 */
class InMemoryTenantTokenStoreFactoryTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    private InMemoryTenantTokenStoreFactory testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new InMemoryTenantTokenStoreFactory();
    }

    @Nested
    class Apply {

        @Test
        void applyReturnsTokenStoreForTenant() {
            // when
            TokenStore tokenStore = testSubject.apply(TENANT_1);

            // then
            assertThat(tokenStore).isNotNull();
        }

        @Test
        void applyReturnsSameTokenStoreForSameTenant() {
            // when
            TokenStore first = testSubject.apply(TENANT_1);
            TokenStore second = testSubject.apply(TENANT_1);

            // then
            assertThat(second).isSameAs(first);
        }

        @Test
        void applyReturnsDifferentTokenStoresForDifferentTenants() {
            // when
            TokenStore tokenStore1 = testSubject.apply(TENANT_1);
            TokenStore tokenStore2 = testSubject.apply(TENANT_2);

            // then
            assertThat(tokenStore1).isNotSameAs(tokenStore2);
        }
    }

    @Nested
    class TokenStoreCount {

        @Test
        void tokenStoreCountReflectsNumberOfCreatedStores() {
            // then
            assertThat(testSubject.tokenStoreCount()).isEqualTo(0);

            // when
            testSubject.apply(TENANT_1);

            // then
            assertThat(testSubject.tokenStoreCount()).isEqualTo(1);

            // when
            testSubject.apply(TENANT_2);

            // then
            assertThat(testSubject.tokenStoreCount()).isEqualTo(2);

            // when - applying same tenant again should not increase count
            testSubject.apply(TENANT_1);

            // then
            assertThat(testSubject.tokenStoreCount()).isEqualTo(2);
        }
    }
}
