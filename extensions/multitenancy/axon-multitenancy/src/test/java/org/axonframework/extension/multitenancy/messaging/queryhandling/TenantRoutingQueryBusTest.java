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

package org.axonframework.extension.multitenancy.messaging.queryhandling;

import org.axonframework.common.Registration;
import org.axonframework.extension.multitenancy.common.TenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link TenantRoutingQueryBus}.
 *
 * @author Stefan Dragisic
 */
class TenantRoutingQueryBusTest {

    private static final String PAYLOAD = "testQuery";
    private static final MessageType QUERY_TYPE = new MessageType("TestQuery");
    private static final MessageType RESPONSE_TYPE = new MessageType("Response");
    private static final QueryMessage TEST_QUERY = new GenericQueryMessage(QUERY_TYPE, PAYLOAD);
    private static final QualifiedName QUERY_NAME = TEST_QUERY.type().qualifiedName();

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    private QueryBus tenantSegment1;
    private QueryBus tenantSegment2;

    private TenantRoutingQueryBus testSubject;

    @BeforeEach
    void setUp() {
        tenantSegment1 = mock(QueryBus.class);
        tenantSegment2 = mock(QueryBus.class);

        // Stub subscribe to return the bus for fluent chaining
        when(tenantSegment1.subscribe(any(QualifiedName.class), any(QueryHandler.class))).thenReturn(tenantSegment1);
        when(tenantSegment2.subscribe(any(QualifiedName.class), any(QueryHandler.class))).thenReturn(tenantSegment2);

        TenantQuerySegmentFactory tenantSegmentFactory = tenant -> {
            if (tenant.tenantId().equals("tenant1")) {
                return tenantSegment1;
            } else {
                return tenantSegment2;
            }
        };

        // Resolver always resolves to tenant2
        TenantResolver<Message> tenantResolver =
                (message, tenants) -> TENANT_2;

        testSubject = new TenantRoutingQueryBus(tenantSegmentFactory, tenantResolver);
    }

    @Nested
    class Query {

        @Test
        void queryRoutesToCorrectTenant() {
            // given
            QueryResponseMessage expectedResponse = new GenericQueryResponseMessage(RESPONSE_TYPE, "result");
            when(tenantSegment2.query(any(), any()))
                    .thenReturn(MessageStream.just(expectedResponse));

            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            // when
            MessageStream<QueryResponseMessage> result = testSubject.query(TEST_QUERY, null);

            // then
            verify(tenantSegment2).query(eq(TEST_QUERY), isNull());
            verify(tenantSegment1, never()).query(any(), any());
            assertThat(result).isNotNull();
        }

        @Test
        void queryToUnknownTenantReturnsFailedStream() {
            // given - no tenants registered

            // when
            MessageStream<QueryResponseMessage> result = testSubject.query(TEST_QUERY, null);

            // then - the stream should be failed
            assertThat(result).isNotNull();
            assertThat(result.error()).isPresent();
        }
    }

    @Nested
    class SubscriptionQuery {

        @Test
        void subscriptionQueryRoutesToCorrectTenant() {
            // given
            QueryResponseMessage expectedResponse = new GenericQueryResponseMessage(RESPONSE_TYPE, "result");
            when(tenantSegment2.subscriptionQuery(any(), any(), anyInt()))
                    .thenReturn(MessageStream.just(expectedResponse));

            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            // when
            MessageStream<QueryResponseMessage> result = testSubject.subscriptionQuery(TEST_QUERY, null, 10);

            // then
            verify(tenantSegment2).subscriptionQuery(eq(TEST_QUERY), isNull(), eq(10));
            verify(tenantSegment1, never()).subscriptionQuery(any(), any(), anyInt());
        }
    }

    @Nested
    class Subscribe {

        @Test
        void subscribeRegistersHandlerOnExistingTenants() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            QueryHandler handler = (msg, ctx) -> MessageStream.just(new GenericQueryResponseMessage(RESPONSE_TYPE, "ok"));

            // when
            testSubject.subscribe(QUERY_NAME, handler);

            // then
            verify(tenantSegment1).subscribe(eq(QUERY_NAME), eq(handler));
            verify(tenantSegment2).subscribe(eq(QUERY_NAME), eq(handler));
        }

        @Test
        void registerAndStartTenantSubscribesExistingHandlers() {
            // given - first subscribe a handler
            QueryHandler handler = (msg, ctx) -> MessageStream.just(new GenericQueryResponseMessage(RESPONSE_TYPE, "ok"));
            testSubject.subscribe(QUERY_NAME, handler);

            // when - then register and start a tenant
            testSubject.registerAndStartTenant(TENANT_1);

            // then - handler should be subscribed to the new tenant segment
            verify(tenantSegment1).subscribe(eq(QUERY_NAME), eq(handler));
        }
    }

    @Nested
    class EmitUpdate {

        @Test
        void emitUpdateRoutesToCorrectTenant() {
            // given
            when(tenantSegment2.emitUpdate(any(), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            // Create a mock ProcessingContext that returns our test query
            ProcessingContext context = mock(ProcessingContext.class);
            when(context.getResource(Message.RESOURCE_KEY)).thenReturn(TEST_QUERY);

            // when
            testSubject.emitUpdate(q -> true, () -> null, context);

            // then - should only emit to tenant2 (resolved from the message)
            verify(tenantSegment2).emitUpdate(any(), any(), any());
            verify(tenantSegment1, never()).emitUpdate(any(), any(), any());
        }

        @Test
        void emitUpdateWithoutContextReturnsFailedFuture() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            // when
            CompletableFuture<Void> result = testSubject.emitUpdate(q -> true, () -> null, null);

            // then
            assertThat(result.isCompletedExceptionally()).isTrue();
            assertThatThrownBy(result::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class CompleteSubscriptions {

        @Test
        void completeSubscriptionsRoutesToCorrectTenant() {
            // given
            when(tenantSegment2.completeSubscriptions(any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            // Create a mock ProcessingContext that returns our test query
            ProcessingContext context = mock(ProcessingContext.class);
            when(context.getResource(Message.RESOURCE_KEY)).thenReturn(TEST_QUERY);

            // when
            testSubject.completeSubscriptions(q -> true, context);

            // then - should only complete on tenant2 (resolved from the message)
            verify(tenantSegment2).completeSubscriptions(any(), any());
            verify(tenantSegment1, never()).completeSubscriptions(any(), any());
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

            // then - should fail because tenant is no longer registered
            MessageStream<QueryResponseMessage> result = testSubject.query(TEST_QUERY, null);
            assertThat(result.error()).isPresent();
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
            assertThatThrownBy(() -> new TenantRoutingQueryBus(null, (m, t) -> TENANT_1))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRequiresTenantResolver() {
            // when / then
            assertThatThrownBy(() -> new TenantRoutingQueryBus(t -> tenantSegment1, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
