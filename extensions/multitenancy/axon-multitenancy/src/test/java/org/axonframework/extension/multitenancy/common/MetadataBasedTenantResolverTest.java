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

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link MetadataBasedTenantResolver}.
 *
 * @author Stefan Dragisic
 */
class MetadataBasedTenantResolverTest {

    private static final String TENANT_ID = "tenant-1";
    private static final String CUSTOM_KEY = "customTenantKey";
    private static final MessageType COMMAND_TYPE = new MessageType("TestCommand");

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant-1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant-2");
    private static final Set<TenantDescriptor> TENANTS = Set.of(TENANT_1, TENANT_2);

    @Nested
    class ResolveTenant {

        @Test
        void resolvesTenantFromDefaultMetadataKey() {
            // given
            MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver();

            CommandMessage message = new GenericCommandMessage(
                    COMMAND_TYPE,
                    "payload",
                    Map.of("tenantId", TENANT_ID)
            );

            // when
            TenantDescriptor result = testSubject.resolveTenant(message, TENANTS);

            // then
            assertThat(result).isEqualTo(TENANT_1);
            assertThat(result.tenantId()).isEqualTo(TENANT_ID);
        }

        @Test
        void resolvesTenantFromCustomMetadataKey() {
            // given
            MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver(CUSTOM_KEY);

            CommandMessage message = new GenericCommandMessage(
                    COMMAND_TYPE,
                    "payload",
                    Map.of(CUSTOM_KEY, TENANT_ID)
            );

            // when
            TenantDescriptor result = testSubject.resolveTenant(message, TENANTS);

            // then
            assertThat(result).isEqualTo(TENANT_1);
        }

        @Test
        void throwsExceptionWhenMetadataKeyNotPresent() {
            // given
            MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver();

            CommandMessage message = new GenericCommandMessage(
                    COMMAND_TYPE,
                    "payload",
                    Collections.emptyMap()
            );

            // when / then
            assertThatThrownBy(() -> testSubject.resolveTenant(message, TENANTS))
                    .isInstanceOf(NoSuchTenantException.class)
                    .hasMessageContaining("tenantId")
                    .hasMessageContaining("metadata");
        }

        @Test
        void throwsExceptionWhenCustomKeyNotPresent() {
            // given
            MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver(CUSTOM_KEY);

            CommandMessage message = new GenericCommandMessage(
                    COMMAND_TYPE,
                    "payload",
                    Map.of("wrongKey", TENANT_ID)
            );

            // when / then
            assertThatThrownBy(() -> testSubject.resolveTenant(message, TENANTS))
                    .isInstanceOf(NoSuchTenantException.class)
                    .hasMessageContaining(CUSTOM_KEY);
        }

        @Test
        void resolvesFromMessageWithMultipleMetadataEntries() {
            // given
            MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver();

            CommandMessage message = new GenericCommandMessage(
                    COMMAND_TYPE,
                    "payload",
                    Map.of(
                            "someOtherKey", "someValue",
                            "tenantId", TENANT_ID,
                            "anotherKey", "anotherValue"
                    )
            );

            // when
            TenantDescriptor result = testSubject.resolveTenant(message, TENANTS);

            // then
            assertThat(result).isEqualTo(TENANT_1);
        }

        @Test
        void createsNewTenantDescriptorForUnknownTenant() {
            // given
            MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver();
            String unknownTenantId = "unknown-tenant";

            CommandMessage message = new GenericCommandMessage(
                    COMMAND_TYPE,
                    "payload",
                    Map.of("tenantId", unknownTenantId)
            );

            // when - the resolver creates a TenantDescriptor regardless of whether
            // it's in the known tenants set - that validation happens elsewhere
            TenantDescriptor result = testSubject.resolveTenant(message, TENANTS);

            // then
            assertThat(result.tenantId()).isEqualTo(unknownTenantId);
        }
    }

    @Nested
    class MetadataKeyConfiguration {

        @Test
        void defaultConstructorUsesDefaultKey() {
            // given
            MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver();

            // then
            assertThat(testSubject.metadataKey()).isEqualTo(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY);
            assertThat(testSubject.metadataKey()).isEqualTo("tenantId");
        }

        @Test
        void metadataKeyAccessorReturnsConfiguredKey() {
            // given
            MetadataBasedTenantResolver testSubject = new MetadataBasedTenantResolver(CUSTOM_KEY);

            // then
            assertThat(testSubject.metadataKey()).isEqualTo(CUSTOM_KEY);
        }
    }

    @Nested
    class ConstructorValidation {

        @Test
        void constructorRejectsNullMetadataKey() {
            // when / then
            assertThatThrownBy(() -> new MetadataBasedTenantResolver(null))
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void constructorRejectsEmptyMetadataKey() {
            // when / then
            assertThatThrownBy(() -> new MetadataBasedTenantResolver(""))
                    .isInstanceOf(AxonConfigurationException.class);
        }
    }
}
