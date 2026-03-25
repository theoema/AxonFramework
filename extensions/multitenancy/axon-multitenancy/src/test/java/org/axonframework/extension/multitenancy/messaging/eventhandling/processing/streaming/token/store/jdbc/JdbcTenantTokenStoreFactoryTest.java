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
package org.axonframework.extension.multitenancy.messaging.eventhandling.processing.streaming.token.store.jdbc;

import org.axonframework.conversion.Converter;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStoreConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link JdbcTenantTokenStoreFactory}.
 *
 * @author Stefan Dragisic
 */
class JdbcTenantTokenStoreFactoryTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    private TenantConnectionProviderFactory connectionProviderFactory;
    private Converter converter;
    private JdbcTenantTokenStoreFactory testSubject;

    @BeforeEach
    void setUp() {
        connectionProviderFactory = mock(TenantConnectionProviderFactory.class);
        converter = mock(Converter.class);

        // Setup mock to return a DataSource for any tenant
        when(connectionProviderFactory.apply(any(TenantDescriptor.class)))
                .thenReturn(mock(DataSource.class));

        testSubject = new JdbcTenantTokenStoreFactory(connectionProviderFactory, converter);
    }

    @Nested
    class Apply {

        @Test
        void applyReturnsJdbcTokenStore() {
            // when
            TokenStore tokenStore = testSubject.apply(TENANT_1);

            // then
            assertThat(tokenStore).isNotNull();
            assertThat(tokenStore).isInstanceOf(JdbcTokenStore.class);
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
            JdbcTokenStoreConfiguration customConfig = JdbcTokenStoreConfiguration.DEFAULT
                    .nodeId("custom-node");

            JdbcTenantTokenStoreFactory factory = new JdbcTenantTokenStoreFactory(
                    connectionProviderFactory, converter, customConfig
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
        void constructorRejectsNullConnectionProviderFactory() {
            // when / then
            assertThatThrownBy(() -> new JdbcTenantTokenStoreFactory(null, converter))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRejectsNullConverter() {
            // when / then
            assertThatThrownBy(() -> new JdbcTenantTokenStoreFactory(connectionProviderFactory, null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRejectsNullConfiguration() {
            // when / then
            assertThatThrownBy(() -> new JdbcTenantTokenStoreFactory(connectionProviderFactory, converter, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class ConnectionProviderCaching {

        @Test
        void applyUsesConnectionProviderFactory() {
            // when
            testSubject.apply(TENANT_1);

            // then
            verify(connectionProviderFactory).apply(TENANT_1);
        }

        @Test
        void applyDoesNotCallConnectionProviderFactoryForCachedTenant() {
            // when
            testSubject.apply(TENANT_1);
            testSubject.apply(TENANT_1);

            // then - should only be called once due to caching
            verify(connectionProviderFactory, times(1)).apply(TENANT_1);
        }
    }
}
