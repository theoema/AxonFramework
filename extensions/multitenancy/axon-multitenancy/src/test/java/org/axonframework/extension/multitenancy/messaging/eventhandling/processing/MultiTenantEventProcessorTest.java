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

package org.axonframework.extension.multitenancy.messaging.eventhandling.processing;

import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class validating the {@link MultiTenantEventProcessor}.
 *
 * @author Stefan Dragisic
 */
class MultiTenantEventProcessorTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant2");

    private EventProcessor tenantSegment1;
    private EventProcessor tenantSegment2;

    private MultiTenantEventProcessor testSubject;

    @BeforeEach
    void setUp() {
        tenantSegment1 = mock(EventProcessor.class);
        tenantSegment2 = mock(EventProcessor.class);

        when(tenantSegment1.start()).thenReturn(CompletableFuture.completedFuture(null));
        when(tenantSegment2.start()).thenReturn(CompletableFuture.completedFuture(null));
        when(tenantSegment1.shutdown()).thenReturn(CompletableFuture.completedFuture(null));
        when(tenantSegment2.shutdown()).thenReturn(CompletableFuture.completedFuture(null));

        TenantEventProcessorSegmentFactory tenantSegmentFactory = tenant -> {
            if (tenant.tenantId().equals("tenant1")) {
                return tenantSegment1;
            } else {
                return tenantSegment2;
            }
        };

        testSubject = new MultiTenantEventProcessor("testProcessor", tenantSegmentFactory);
    }

    @Nested
    class Registration {

        @Test
        void nameReturnsProcessorName() {
            // then
            assertThat(testSubject.name()).isEqualTo("testProcessor");
        }

        @Test
        void registerTenantAddsTenantSegment() {
            // when
            testSubject.registerTenant(TENANT_1);

            // then
            assertThat(testSubject.tenantSegments()).hasSize(1);
            assertThat(testSubject.tenantSegments()).containsKey(TENANT_1);
            assertThat(testSubject.tenantEventProcessors()).contains(tenantSegment1);
        }

        @Test
        void registerTenantAfterStartThrowsException() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.start();

            // when / then
            assertThatThrownBy(() -> testSubject.registerTenant(TENANT_2))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void registerAndStartTenantStartsSegment() {
            // when
            testSubject.registerAndStartTenant(TENANT_1);

            // then
            verify(tenantSegment1).start();
            assertThat(testSubject.tenantEventProcessors()).contains(tenantSegment1);
        }
    }

    @Nested
    class Lifecycle {

        @Test
        void startStartsAllTenantSegments() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            // when
            CompletableFuture<Void> result = testSubject.start();

            // then
            assertThat(result.isDone()).isTrue();
            verify(tenantSegment1).start();
            verify(tenantSegment2).start();
            assertThat(testSubject.isRunning()).isTrue();
        }

        @Test
        void shutdownShutdownsAllTenantSegments() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);
            testSubject.start();

            // when
            CompletableFuture<Void> result = testSubject.shutdown();

            // then
            assertThat(result.isDone()).isTrue();
            verify(tenantSegment1).shutdown();
            verify(tenantSegment2).shutdown();
            assertThat(testSubject.isRunning()).isFalse();
        }
    }

    @Nested
    class TenantStatus {

        @Test
        void isRunningForTenantDelegatesToTenantSegment() {
            // given
            when(tenantSegment1.isRunning()).thenReturn(true);
            testSubject.registerTenant(TENANT_1);

            // when / then
            assertThat(testSubject.isRunning(TENANT_1)).isTrue();
            verify(tenantSegment1).isRunning();
        }

        @Test
        void isErrorForTenantDelegatesToTenantSegment() {
            // given
            when(tenantSegment1.isError()).thenReturn(true);
            testSubject.registerTenant(TENANT_1);

            // when / then
            assertThat(testSubject.isError(TENANT_1)).isTrue();
            verify(tenantSegment1).isError();
        }

        @Test
        void isErrorReturnsFalseForMultiTenantProcessor() {
            // when / then - the multi-tenant processor itself never reports error
            // Individual tenant segments should be checked via isError(TenantDescriptor)
            assertThat(testSubject.isError()).isFalse();
        }
    }

    @Nested
    class TenantRemoval {

        @Test
        void stopAndRemoveTenantShutdownsAndRemovesSegment() {
            // given
            testSubject.registerAndStartTenant(TENANT_1);

            // when
            boolean removed = testSubject.stopAndRemoveTenant(TENANT_1);

            // then
            assertThat(removed).isTrue();
            verify(tenantSegment1).shutdown();
            assertThat(testSubject.tenantSegments()).doesNotContainKey(TENANT_1);
            assertThat(testSubject.tenantEventProcessors()).isEmpty();
        }

        @Test
        void stopAndRemoveTenantReturnsFalseForUnknownTenant() {
            // when
            boolean removed = testSubject.stopAndRemoveTenant(TENANT_1);

            // then
            assertThat(removed).isFalse();
        }

        @Test
        void unregisterTenantViaCancelStopsAndRemoves() {
            // given
            testSubject.registerAndStartTenant(TENANT_1);

            // when
            testSubject.registerAndStartTenant(TENANT_1).cancel();

            // then - since registerAndStartTenant returns a registration that calls stopAndRemoveTenant
            verify(tenantSegment1).shutdown();
        }

        @Test
        void tenantEventProcessorsReturnsUnmodifiableList() {
            // given
            testSubject.registerTenant(TENANT_1);

            // when / then
            assertThatThrownBy(() -> testSubject.tenantEventProcessors().add(tenantSegment2))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    class ConstructorValidation {

        @Test
        void constructorRequiresName() {
            // when / then
            assertThatThrownBy(() -> new MultiTenantEventProcessor(null, t -> tenantSegment1))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRequiresTenantSegmentFactory() {
            // when / then
            assertThatThrownBy(() -> new MultiTenantEventProcessor("testProcessor", null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
