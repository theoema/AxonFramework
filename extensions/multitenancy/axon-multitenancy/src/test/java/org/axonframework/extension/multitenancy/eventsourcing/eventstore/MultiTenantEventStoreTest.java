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

package org.axonframework.extension.multitenancy.eventsourcing.eventstore;

import org.axonframework.common.Registration;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.extension.multitenancy.common.NoSuchTenantException;
import org.axonframework.extension.multitenancy.common.TargetTenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link MultiTenantEventStore}.
 *
 * @author Stefan Dragisic
 */
class MultiTenantEventStoreTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    private EventStore tenantSegment1;
    private EventStore tenantSegment2;

    private MultiTenantEventStore testSubject;

    @BeforeEach
    void setUp() {
        tenantSegment1 = mock(EventStore.class);
        tenantSegment2 = mock(EventStore.class);

        when(tenantSegment1.subscribe(any())).thenReturn(() -> true);
        when(tenantSegment2.subscribe(any())).thenReturn(() -> true);

        TenantEventSegmentFactory tenantSegmentFactory = tenant -> {
            if (tenant.tenantId().equals("tenant1")) {
                return tenantSegment1;
            } else {
                return tenantSegment2;
            }
        };

        // Resolver always resolves to tenant2
        TargetTenantResolver<Message> targetTenantResolver =
                (message, tenants) -> TENANT_2;

        testSubject = new MultiTenantEventStore(tenantSegmentFactory, targetTenantResolver);
    }

    @Nested
    class Publish {

        @Test
        void publishRoutesToCorrectTenant() {
            // given
            when(tenantSegment2.publish(any(), anyList()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");

            // when
            CompletableFuture<Void> result = testSubject.publish(null, List.of(event));

            // then
            assertThat(result.isDone()).isTrue();
            verify(tenantSegment2).publish(isNull(), eq(List.of(event)));
            verify(tenantSegment1, never()).publish(any(), anyList());
        }

        @Test
        void publishEmptyListReturnsImmediately() {
            // given
            testSubject.registerTenant(TENANT_1);

            // when
            CompletableFuture<Void> result = testSubject.publish(null, List.of());

            // then
            assertThat(result.isDone()).isTrue();
            verify(tenantSegment1, never()).publish(any(), anyList());
            verify(tenantSegment2, never()).publish(any(), anyList());
        }

        @Test
        void publishToUnknownTenantThrowsException() {
            // given - no tenants registered
            EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");

            // when / then
            assertThatThrownBy(() -> testSubject.publish(null, List.of(event)))
                    .isInstanceOf(NoSuchTenantException.class);
        }
    }

    @Nested
    class Subscribe {

        @Test
        void subscribeRegistersOnExistingTenants() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            // when
            testSubject.subscribe((events, ctx) -> CompletableFuture.completedFuture(null));

            // then
            verify(tenantSegment1).subscribe(any());
            verify(tenantSegment2).subscribe(any());
        }

        @Test
        void registerAndStartTenantSubscribesExistingConsumers() {
            // given - first subscribe a consumer
            testSubject.subscribe((events, ctx) -> CompletableFuture.completedFuture(null));

            // when - then register and start a tenant
            testSubject.registerAndStartTenant(TENANT_1);

            // then - consumer should be subscribed to the new tenant segment
            verify(tenantSegment1).subscribe(any());
        }
    }

    @Nested
    class UnsupportedStreamingOperations {

        @Test
        void openStreamThrowsUnsupportedOperationException() {
            // given
            testSubject.registerTenant(TENANT_1);

            // when / then
            StreamingCondition condition = StreamingCondition.startingFrom(null);
            assertThatThrownBy(() -> testSubject.open(condition, null))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void firstTokenThrowsUnsupportedOperationException() {
            // given
            testSubject.registerTenant(TENANT_1);

            // when / then
            assertThatThrownBy(() -> testSubject.firstToken(null))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void latestTokenThrowsUnsupportedOperationException() {
            // given
            testSubject.registerTenant(TENANT_1);

            // when / then
            assertThatThrownBy(() -> testSubject.latestToken(null))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void tokenAtThrowsUnsupportedOperationException() {
            // given
            testSubject.registerTenant(TENANT_1);

            // when / then
            assertThatThrownBy(() -> testSubject.tokenAt(java.time.Instant.now(), null))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    class TenantManagement {

        @Test
        void unregisterTenantRemovesTenantFromRouting() {
            // given
            Registration registration = testSubject.registerTenant(TENANT_2);

            // when - unregister the tenant
            registration.cancel();

            // then - should throw because tenant is no longer registered
            EventMessage event = new GenericEventMessage(new MessageType("TestEvent"), "payload");
            assertThatThrownBy(() -> testSubject.publish(null, List.of(event)))
                    .isInstanceOf(NoSuchTenantException.class);
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
            assertThatThrownBy(() -> new MultiTenantEventStore(null, (m, t) -> TENANT_1))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRequiresTargetTenantResolver() {
            // when / then
            assertThatThrownBy(() -> new MultiTenantEventStore(t -> tenantSegment1, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
