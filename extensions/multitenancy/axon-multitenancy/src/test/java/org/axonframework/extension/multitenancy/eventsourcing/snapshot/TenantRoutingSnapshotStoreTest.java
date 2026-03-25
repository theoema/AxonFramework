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

package org.axonframework.extension.multitenancy.eventsourcing.snapshot;

import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.extension.multitenancy.common.NoSuchTenantException;
import org.axonframework.extension.multitenancy.common.TenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link TenantRoutingSnapshotStore}.
 *
 * @author Theo Emanuelsson
 */
class TenantRoutingSnapshotStoreTest {

    private static final String TENANT_KEY = "tenantId";
    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");
    private static final QualifiedName AGGREGATE_NAME = new QualifiedName("test.Aggregate");

    private InMemorySnapshotStore tenant1Store;
    private InMemorySnapshotStore tenant2Store;
    private TenantRoutingSnapshotStore testSubject;

    @BeforeEach
    void setUp() {
        tenant1Store = new InMemorySnapshotStore();
        tenant2Store = new InMemorySnapshotStore();

        TenantSnapshotStoreSegmentFactory segmentFactory = tenant -> {
            if (tenant.tenantId().equals("tenant1")) {
                return tenant1Store;
            }
            return tenant2Store;
        };

        TenantResolver<Message> tenantResolver =
                (message, tenants) -> TenantDescriptor.tenantWithId(message.metadata().get(TENANT_KEY));

        testSubject = new TenantRoutingSnapshotStore(segmentFactory, tenantResolver);

        testSubject.registerTenant(TENANT_1);
        testSubject.registerTenant(TENANT_2);
    }

    /**
     * Creates a minimal {@link ProcessingContext} with a message containing the given tenant ID in its metadata.
     * Uses a simple map-backed implementation to avoid depending on test-jar utilities.
     */
    private ProcessingContext contextForTenant(String tenantId) {
        Message message = new GenericEventMessage(
                new MessageType("TestEvent"), "payload",
                Metadata.with(TENANT_KEY, tenantId)
        );
        return new SimpleProcessingContext(message);
    }

    private Snapshot testSnapshot() {
        return new Snapshot(new GlobalIndexPosition(1L), "1", "state", Instant.now(), Map.of());
    }

    @Nested
    class StoreOperation {

        @Test
        void routesToCorrectTenantStore() {
            // given
            Snapshot snapshot = testSnapshot();
            ProcessingContext context = contextForTenant("tenant1");

            // when
            testSubject.store(AGGREGATE_NAME, "id-1", snapshot, context).join();

            // then
            assertThat(tenant1Store.load(AGGREGATE_NAME, "id-1", null).join()).isEqualTo(snapshot);
            assertThat(tenant2Store.load(AGGREGATE_NAME, "id-1", null).join()).isNull();
        }

        @Test
        void routesToTenant2WhenMetadataIndicatesTenant2() {
            // given
            Snapshot snapshot = testSnapshot();
            ProcessingContext context = contextForTenant("tenant2");

            // when
            testSubject.store(AGGREGATE_NAME, "id-1", snapshot, context).join();

            // then
            assertThat(tenant2Store.load(AGGREGATE_NAME, "id-1", null).join()).isEqualTo(snapshot);
            assertThat(tenant1Store.load(AGGREGATE_NAME, "id-1", null).join()).isNull();
        }
    }

    @Nested
    class LoadOperation {

        @Test
        void routesToCorrectTenantStore() {
            // given
            Snapshot snapshot = testSnapshot();
            tenant1Store.store(AGGREGATE_NAME, "id-1", snapshot, null).join();

            // when
            ProcessingContext context = contextForTenant("tenant1");
            Snapshot result = testSubject.load(AGGREGATE_NAME, "id-1", context).join();

            // then
            assertThat(result).isEqualTo(snapshot);
        }

        @Test
        void returnsNullWhenSnapshotOnlyExistsInOtherTenant() {
            // given
            Snapshot snapshot = testSnapshot();
            tenant1Store.store(AGGREGATE_NAME, "id-1", snapshot, null).join();

            // when
            ProcessingContext context = contextForTenant("tenant2");
            Snapshot result = testSubject.load(AGGREGATE_NAME, "id-1", context).join();

            // then
            assertThat(result).isNull();
        }
    }

    @Nested
    class TenantResolutionErrors {

        @Test
        void throwsWhenNoProcessingContextProvided() {
            // given
            Snapshot snapshot = testSnapshot();

            // when / then
            assertThatThrownBy(() -> testSubject.store(AGGREGATE_NAME, "id-1", snapshot, null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no ProcessingContext available");
        }

        @Test
        void throwsWhenNoMessageInContext() {
            // given - a context without a message resource
            ProcessingContext emptyContext = new SimpleProcessingContext(null);
            Snapshot snapshot = testSnapshot();

            // when / then
            assertThatThrownBy(() -> testSubject.store(AGGREGATE_NAME, "id-1", snapshot, emptyContext))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no message found in ProcessingContext");
        }

        @Test
        void throwsNoSuchTenantExceptionForUnknownTenant() {
            // given
            ProcessingContext context = contextForTenant("unknownTenant");

            // when / then
            assertThatThrownBy(() -> testSubject.load(AGGREGATE_NAME, "id-1", context))
                    .isInstanceOf(NoSuchTenantException.class);
        }
    }

    @Nested
    class TenantRegistration {

        @Test
        void registerTenantCreatesSegment() {
            // given
            TenantRoutingSnapshotStore emptyStore = new TenantRoutingSnapshotStore(
                    t -> new InMemorySnapshotStore(), (m, t) -> TENANT_1
            );

            // when
            emptyStore.registerTenant(TENANT_1);

            // then
            assertThat(emptyStore.tenantSegments()).containsKey(TENANT_1);
            assertThat(emptyStore.tenantSegments()).hasSize(1);
        }

        @Test
        void unregisterTenantRemovesSegment() {
            // given / when
            var registration = testSubject.registerTenant(TenantDescriptor.tenantWithId("tenant3"));
            assertThat(testSubject.tenantSegments()).hasSize(3);

            registration.cancel();

            // then
            assertThat(testSubject.tenantSegments()).hasSize(2);
            assertThat(testSubject.tenantSegments()).doesNotContainKey(TenantDescriptor.tenantWithId("tenant3"));
        }

        @Test
        void registerAndStartTenantCreatesSegment() {
            // given
            TenantRoutingSnapshotStore emptyStore = new TenantRoutingSnapshotStore(
                    t -> new InMemorySnapshotStore(), (m, t) -> TENANT_1
            );

            // when
            emptyStore.registerAndStartTenant(TENANT_1);

            // then
            assertThat(emptyStore.tenantSegments()).containsKey(TENANT_1);
        }

        @Test
        void tenantSegmentsReturnsAllRegistered() {
            // when / then
            assertThat(testSubject.tenantSegments()).containsKey(TENANT_1);
            assertThat(testSubject.tenantSegments()).containsKey(TENANT_2);
            assertThat(testSubject.tenantSegments()).hasSize(2);
        }
    }

    @Nested
    class ConstructorValidation {

        @Test
        void failsWithoutTenantSegmentFactory() {
            assertThatThrownBy(() ->
                    new TenantRoutingSnapshotStore(null, (m, t) -> TENANT_1)
            ).isInstanceOf(NullPointerException.class);
        }

        @Test
        void failsWithoutTenantResolver() {
            assertThatThrownBy(() ->
                    new TenantRoutingSnapshotStore(t -> new InMemorySnapshotStore(), null)
            ).isInstanceOf(NullPointerException.class);
        }
    }

    /**
     * Minimal {@link ProcessingContext} implementation for testing tenant resolution.
     * Only supports resource storage/retrieval needed by {@link Message#fromContext(ProcessingContext)}.
     */
    @SuppressWarnings("NullableProblems")
    private static class SimpleProcessingContext implements ProcessingContext {

        private final Map<ResourceKey<?>, Object> resources = new ConcurrentHashMap<>();

        SimpleProcessingContext(Message message) {
            if (message != null) {
                resources.put(Message.RESOURCE_KEY, message);
            }
        }

        @Override
        public boolean isStarted() {
            return false;
        }

        @Override
        public boolean isError() {
            return false;
        }

        @Override
        public boolean isCommitted() {
            return false;
        }

        @Override
        public boolean isCompleted() {
            return false;
        }

        @Override
        public ProcessingLifecycle on(Phase phase,
                                      Function<ProcessingContext, CompletableFuture<?>> action) {
            return this;
        }

        @Override
        public ProcessingLifecycle onError(ErrorHandler action) {
            return this;
        }

        @Override
        public ProcessingLifecycle whenComplete(Consumer<ProcessingContext> action) {
            return this;
        }

        @Override
        public boolean containsResource(ResourceKey<?> key) {
            return resources.containsKey(key);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getResource(ResourceKey<T> key) {
            return (T) resources.get(key);
        }

        @Override
        public <T> ProcessingContext withResource(ResourceKey<T> key, T resource) {
            resources.put(key, resource);
            return this;
        }

        @Override
        public Map<ResourceKey<?>, Object> resources() {
            return Map.copyOf(resources);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T putResource(ResourceKey<T> key, T resource) {
            return (T) resources.put(key, resource);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T updateResource(ResourceKey<T> key,
                                    UnaryOperator<T> resourceUpdater) {
            return (T) resources.compute(key, (k, v) -> resourceUpdater.apply((T) v));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T putResourceIfAbsent(ResourceKey<T> key, T resource) {
            return (T) resources.putIfAbsent(key, resource);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T computeResourceIfAbsent(ResourceKey<T> key,
                                             Supplier<T> resourceSupplier) {
            return (T) resources.computeIfAbsent(key, k -> resourceSupplier.get());
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T removeResource(ResourceKey<T> key) {
            return (T) resources.remove(key);
        }

        @Override
        public <T> boolean removeResource(ResourceKey<T> key, T expectedResource) {
            return resources.remove(key, expectedResource);
        }

        @Override
        public <C> C component(Class<C> type, String name) {
            throw new UnsupportedOperationException("Not needed for these tests");
        }
    }
}
