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

package org.axonframework.extension.multitenancy.messaging.commandhandling;

import org.axonframework.extension.multitenancy.common.NoSuchTenantException;
import org.axonframework.extension.multitenancy.common.TargetTenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link MultiTenantCommandBus}.
 *
 * @author Stefan Dragisic
 */
class MultiTenantCommandBusTest {

    private static final String PAYLOAD = "testCommand";
    private static final MessageType COMMAND_TYPE = new MessageType("TestCommand");
    private static final CommandMessage TEST_COMMAND = new GenericCommandMessage(COMMAND_TYPE, PAYLOAD);
    private static final QualifiedName COMMAND_NAME = TEST_COMMAND.type().qualifiedName();

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    private CommandBus tenantSegment1;
    private CommandBus tenantSegment2;

    private MultiTenantCommandBus testSubject;

    @BeforeEach
    void setUp() {
        tenantSegment1 = mock(CommandBus.class);
        tenantSegment2 = mock(CommandBus.class);

        // Stub subscribe to return the bus for fluent chaining
        when(tenantSegment1.subscribe(any(QualifiedName.class), any(CommandHandler.class))).thenReturn(tenantSegment1);
        when(tenantSegment2.subscribe(any(QualifiedName.class), any(CommandHandler.class))).thenReturn(tenantSegment2);

        TenantCommandSegmentFactory tenantSegmentFactory = tenant -> {
            if (tenant.tenantId().equals("tenant1")) {
                return tenantSegment1;
            } else {
                return tenantSegment2;
            }
        };

        // Resolver always resolves to tenant2
        TargetTenantResolver<Message> targetTenantResolver =
                (message, tenants) -> TENANT_2;

        testSubject = new MultiTenantCommandBus(tenantSegmentFactory, targetTenantResolver);
    }

    @Nested
    class Dispatch {

        @Test
        void dispatchRoutesToCorrectTenant() {
            // given
            CommandResultMessage expectedResult = new GenericCommandResultMessage(COMMAND_TYPE, "result");
            when(tenantSegment2.dispatch(any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(expectedResult));

            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            // when
            CompletableFuture<CommandResultMessage> result = testSubject.dispatch(TEST_COMMAND, null);

            // then
            verify(tenantSegment2).dispatch(eq(TEST_COMMAND), isNull());
            verify(tenantSegment1, never()).dispatch(any(), any());
            assertThat(result.isDone()).isTrue();
        }

        @Test
        void dispatchToUnknownTenantReturnsFailedFuture() throws ExecutionException, InterruptedException {
            // given - no tenants registered

            // when
            CompletableFuture<CommandResultMessage> result = testSubject.dispatch(TEST_COMMAND, null);

            // then
            assertThat(result.isCompletedExceptionally()).isTrue();
            assertThatThrownBy(result::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(NoSuchTenantException.class)
                    .hasMessageContaining("Tenant with identifier [tenant2] is unknown");
        }
    }

    @Nested
    class Subscribe {

        @Test
        void subscribeRegistersHandlerOnExistingTenants() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            CommandHandler handler = (msg, ctx) -> MessageStream.just(new GenericCommandResultMessage(COMMAND_TYPE, "ok"));

            // when
            testSubject.subscribe(COMMAND_NAME, handler);

            // then
            verify(tenantSegment1).subscribe(eq(COMMAND_NAME), eq(handler));
            verify(tenantSegment2).subscribe(eq(COMMAND_NAME), eq(handler));
        }

        @Test
        void registerAndStartTenantSubscribesExistingHandlers() {
            // given - first subscribe a handler
            CommandHandler handler = (msg, ctx) -> MessageStream.just(new GenericCommandResultMessage(COMMAND_TYPE, "ok"));
            testSubject.subscribe(COMMAND_NAME, handler);

            // when - then register and start a tenant
            testSubject.registerAndStartTenant(TENANT_1);

            // then - handler should be subscribed to the new tenant segment
            verify(tenantSegment1).subscribe(eq(COMMAND_NAME), eq(handler));
        }
    }

    @Nested
    class TenantManagement {

        @Test
        void unregisterTenantRemovesTenantFromRouting() {
            // given
            testSubject.registerTenant(TENANT_2);

            // when - unregister the tenant
            testSubject.registerTenant(TENANT_2).cancel();

            // then - should fail because tenant is no longer registered
            CompletableFuture<CommandResultMessage> result = testSubject.dispatch(TEST_COMMAND, null);
            assertThat(result.isCompletedExceptionally()).isTrue();
            assertThatThrownBy(result::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(NoSuchTenantException.class);
        }

        @Test
        void tenantSegmentsReturnsRegisteredTenants() {
            // when
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            // then
            assertThat(testSubject.tenantSegments()).hasSize(2);
            assertThat(testSubject.tenantSegments()).containsKey(TENANT_1);
            assertThat(testSubject.tenantSegments()).containsKey(TENANT_2);
        }
    }

    @Nested
    class ConstructorValidation {

        @Test
        void constructorRequiresTenantSegmentFactory() {
            // when / then
            assertThatThrownBy(() -> new MultiTenantCommandBus(null, (m, t) -> TENANT_1))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRequiresTargetTenantResolver() {
            // when / then
            assertThatThrownBy(() -> new MultiTenantCommandBus(t -> tenantSegment1, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
