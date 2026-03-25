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
package org.axonframework.extension.multitenancy.messaging.eventhandling.processing.streaming.token.store.jpa;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.conversion.Converter;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link JpaTenantTokenStoreFactory}.
 *
 * @author Stefan Dragisic
 * @author Theo Emanuelsson
 */
class JpaTenantTokenStoreFactoryTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    private Function<TenantDescriptor, EntityManagerFactory> emfProvider;
    private Converter converter;
    private JpaTenantTokenStoreFactory testSubject;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        emfProvider = mock(Function.class);
        converter = mock(Converter.class);

        // Setup mock to return an EntityManagerFactory for any tenant
        EntityManagerFactory mockEmf = mock(EntityManagerFactory.class);
        when(mockEmf.createEntityManager()).thenReturn(mock(EntityManager.class));
        when(emfProvider.apply(any(TenantDescriptor.class))).thenReturn(mockEmf);

        testSubject = new JpaTenantTokenStoreFactory(emfProvider, converter);
    }

    @Nested
    class Apply {

        @Test
        void applyReturnsJpaTokenStore() {
            // when
            TokenStore tokenStore = testSubject.apply(TENANT_1);

            // then
            assertThat(tokenStore).isNotNull();
            assertThat(tokenStore).isInstanceOf(JpaTokenStore.class);
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

    @Nested
    class Configuration {

        @Test
        void constructorWithCustomConfiguration() {
            // given
            JpaTokenStoreConfiguration customConfig = JpaTokenStoreConfiguration.DEFAULT
                    .nodeId("custom-node");

            JpaTenantTokenStoreFactory factory = new JpaTenantTokenStoreFactory(
                    emfProvider, converter, customConfig
            );

            // when
            TokenStore tokenStore = factory.apply(TENANT_1);

            // then
            assertThat(tokenStore).isNotNull();
        }
    }

    @Nested
    class ConstructorValidation {

        @Test
        void constructorRejectsNullEmfProvider() {
            // when / then
            assertThatThrownBy(() -> new JpaTenantTokenStoreFactory(null, converter))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRejectsNullConverter() {
            // when / then
            assertThatThrownBy(() -> new JpaTenantTokenStoreFactory(emfProvider, null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRejectsNullConfiguration() {
            // when / then
            assertThatThrownBy(() -> new JpaTenantTokenStoreFactory(emfProvider, converter, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class EmfProviderCaching {

        @Test
        void emfProviderIsCalledForEachNewTenant() {
            // when
            testSubject.apply(TENANT_1);
            testSubject.apply(TENANT_2);
            testSubject.apply(TENANT_1); // Should not call provider again

            // then
            verify(emfProvider, times(1)).apply(TENANT_1);
            verify(emfProvider, times(1)).apply(TENANT_2);
        }
    }
}
