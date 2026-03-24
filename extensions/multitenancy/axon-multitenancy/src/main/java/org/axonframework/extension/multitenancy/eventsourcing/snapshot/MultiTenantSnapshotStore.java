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

import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.extension.multitenancy.common.MultiTenantAwareComponent;
import org.axonframework.extension.multitenancy.common.NoSuchTenantException;
import org.axonframework.extension.multitenancy.common.TargetTenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tenant aware implementation of {@link SnapshotStore} that routes snapshot operations
 * to tenant-specific stores based on the message in the {@link ProcessingContext}.
 * <p>
 * Follows the same pattern as {@link org.axonframework.extension.multitenancy.eventsourcing.eventstore.MultiTenantEventStore}:
 * the tenant is resolved from the current message's metadata via the {@link TargetTenantResolver},
 * and the operation is delegated to the corresponding tenant's {@link SnapshotStore} segment.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 */
public class MultiTenantSnapshotStore implements SnapshotStore, MultiTenantAwareComponent, DescribableComponent {

    /**
     * The order in which the {@link MultiTenantSnapshotStore} is applied as a decorator to the {@link SnapshotStore}.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 50;

    private final Map<TenantDescriptor, SnapshotStore> tenantSegments = new ConcurrentHashMap<>();
    private final TenantSnapshotStoreSegmentFactory tenantSegmentFactory;
    private final TargetTenantResolver<Message> targetTenantResolver;

    /**
     * Instantiate a {@link MultiTenantSnapshotStore} with the given {@code tenantSegmentFactory} and
     * {@code targetTenantResolver}.
     *
     * @param tenantSegmentFactory  the factory to create tenant-specific {@link SnapshotStore} segments
     * @param targetTenantResolver  the resolver to determine the target tenant from a {@link Message}
     */
    public MultiTenantSnapshotStore(TenantSnapshotStoreSegmentFactory tenantSegmentFactory,
                                    TargetTenantResolver<Message> targetTenantResolver) {
        this.tenantSegmentFactory = Objects.requireNonNull(tenantSegmentFactory,
                                                           "TenantSnapshotStoreSegmentFactory may not be null");
        this.targetTenantResolver = Objects.requireNonNull(targetTenantResolver,
                                                           "TargetTenantResolver may not be null");
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot,
                                         @Nullable ProcessingContext context) {
        return resolveTenantStore(context).store(qualifiedName, identifier, snapshot, context);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                      @Nullable ProcessingContext context) {
        return resolveTenantStore(context).load(qualifiedName, identifier, context);
    }

    private SnapshotStore resolveTenantStore(@Nullable ProcessingContext context) {
        if (context == null) {
            throw new IllegalStateException(
                    "Cannot resolve tenant for snapshot operation: no ProcessingContext available."
            );
        }
        Message message = Message.fromContext(context);
        if (message == null) {
            throw new IllegalStateException(
                    "Cannot resolve tenant for snapshot operation: no message found in ProcessingContext."
            );
        }
        TenantDescriptor tenant = targetTenantResolver.resolveTenant(message, tenantSegments.keySet());
        SnapshotStore tenantStore = tenantSegments.get(tenant);
        if (tenantStore == null) {
            throw NoSuchTenantException.forTenantId(tenant.tenantId());
        }
        return tenantStore;
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenantSegmentFactory);
        return () -> tenantSegments.remove(tenantDescriptor) != null;
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return registerTenant(tenantDescriptor);
    }

    /**
     * Returns the tenant segments managed by this store.
     *
     * @return a map of {@link TenantDescriptor} to {@link SnapshotStore}
     */
    public Map<TenantDescriptor, SnapshotStore> tenantSegments() {
        return tenantSegments;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantSegments", tenantSegments);
    }
}
