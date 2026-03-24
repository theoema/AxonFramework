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
package org.axonframework.extension.multitenancy.eventsourcing.eventstore.jpa;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
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
 * Test class validating the {@link JpaTenantEventSegmentFactory}.
 *
 * @author Theo Emanuelsson
 */
class JpaTenantEventSegmentFactoryTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    private Function<TenantDescriptor, EntityManagerFactory> emfProvider;
    private EventConverter eventConverter;
    private JpaTenantEventSegmentFactory testSubject;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        emfProvider = mock(Function.class);
        eventConverter = mock(EventConverter.class);

        // Setup mock to return an EntityManagerFactory for any tenant
        EntityManagerFactory mockEmf = mock(EntityManagerFactory.class);
        when(mockEmf.createEntityManager()).thenReturn(mock(EntityManager.class));
        when(emfProvider.apply(any(TenantDescriptor.class))).thenReturn(mockEmf);

        testSubject = new JpaTenantEventSegmentFactory(emfProvider, eventConverter);
    }

    @Nested
    class Apply {

        @Test
        void applyReturnsStorageEngineBackedEventStore() {
            // when
            EventStore eventStore = testSubject.apply(TENANT_1);

            // then
            assertThat(eventStore).isNotNull();
            assertThat(eventStore).isInstanceOf(StorageEngineBackedEventStore.class);
        }

        @Test
        void applyReturnsSameEventStoreForSameTenant() {
            // when
            EventStore first = testSubject.apply(TENANT_1);
            EventStore second = testSubject.apply(TENANT_1);

            // then
            assertThat(second).isSameAs(first);
        }

        @Test
        void applyReturnsDifferentEventStoresForDifferentTenants() {
            // when
            EventStore eventStore1 = testSubject.apply(TENANT_1);
            EventStore eventStore2 = testSubject.apply(TENANT_2);

            // then
            assertThat(eventStore1).isNotSameAs(eventStore2);
        }
    }

    @Nested
    class EventStoreCount {

        @Test
        void eventStoreCountReflectsNumberOfCreatedStores() {
            // then
            assertThat(testSubject.eventStoreCount()).isEqualTo(0);

            // when
            testSubject.apply(TENANT_1);

            // then
            assertThat(testSubject.eventStoreCount()).isEqualTo(1);

            // when
            testSubject.apply(TENANT_2);

            // then
            assertThat(testSubject.eventStoreCount()).isEqualTo(2);

            // when - applying same tenant again should not increase count
            testSubject.apply(TENANT_1);

            // then
            assertThat(testSubject.eventStoreCount()).isEqualTo(2);
        }
    }

    @Nested
    class Configuration {

        @Test
        void constructorWithCustomConfiguration() {
            // given
            TagResolver customTagResolver = new AnnotationBasedTagResolver();

            JpaTenantEventSegmentFactory factory = new JpaTenantEventSegmentFactory(
                    emfProvider,
                    eventConverter,
                    c -> c.batchSize(50),
                    customTagResolver
            );

            // when
            EventStore eventStore = factory.apply(TENANT_1);

            // then
            assertThat(eventStore).isNotNull();
        }
    }

    @Nested
    class ConstructorValidation {

        @Test
        void constructorRejectsNullEmfProvider() {
            // when / then
            assertThatThrownBy(() -> new JpaTenantEventSegmentFactory(null, eventConverter))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRejectsNullEventConverter() {
            // when / then
            assertThatThrownBy(() -> new JpaTenantEventSegmentFactory(emfProvider, null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void fullConstructorRejectsNullConfigurer() {
            // when / then
            assertThatThrownBy(() -> new JpaTenantEventSegmentFactory(
                    emfProvider,
                    eventConverter,
                    null,
                    new AnnotationBasedTagResolver()
            )).isInstanceOf(NullPointerException.class);
        }

        @Test
        void fullConstructorRejectsNullTagResolver() {
            // when / then
            assertThatThrownBy(() -> new JpaTenantEventSegmentFactory(
                    emfProvider,
                    eventConverter,
                    c -> c,
                    null
            )).isInstanceOf(NullPointerException.class);
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
