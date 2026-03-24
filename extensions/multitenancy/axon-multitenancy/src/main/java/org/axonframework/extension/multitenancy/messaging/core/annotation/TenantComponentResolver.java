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
package org.axonframework.extension.multitenancy.messaging.core.annotation;

import org.axonframework.extension.multitenancy.common.TargetTenantResolver;
import org.axonframework.extension.multitenancy.common.TenantComponentRegistry;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link ParameterResolver} that resolves parameters to tenant-scoped component instances.
 * <p>
 * This resolver uses the configured {@link TargetTenantResolver} to determine which tenant
 * the message belongs to, then retrieves the component instance from the
 * {@link TenantComponentRegistry}. This enables async-safe multi-tenant component
 * access without ThreadLocal storage.
 * <p>
 * When a {@code requestedType} is provided (for subtype resolution), the resolver uses
 * {@link TenantComponentRegistry#getComponent(TenantDescriptor, Class)} to create a
 * type-specific instance. This allows a single factory registered for a base type
 * (e.g., {@code Repository}) to serve any subtype (e.g., {@code OrderRepository}).
 * <p>
 * Usage in an event handler:
 * <pre>{@code
 * @EventHandler
 * public void on(OrderCreatedEvent event, OrderRepository repository) {
 *     // Repository is automatically scoped to the event's tenant
 *     repository.save(new OrderProjection(event));
 * }
 * }</pre>
 *
 * @param <T> the type of component this resolver provides
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see TenantComponentRegistry
 * @see TenantComponentResolverFactory
 */
public class TenantComponentResolver<T> implements ParameterResolver<T> {

    private final TenantComponentRegistry<T> registry;
    private final TargetTenantResolver<Message> tenantResolver;
    private final @Nullable Class<? extends T> requestedType;

    /**
     * Creates a new {@link TenantComponentResolver} for exact type matching.
     *
     * @param registry       the registry for tenant-scoped components
     * @param tenantResolver the resolver used to determine which tenant a message belongs to
     */
    public TenantComponentResolver(TenantComponentRegistry<T> registry,
                                   TargetTenantResolver<Message> tenantResolver) {
        this(registry, tenantResolver, null);
    }

    /**
     * Creates a new {@link TenantComponentResolver} with optional subtype resolution.
     * <p>
     * When {@code requestedType} is non-null, the resolver uses
     * {@link TenantComponentRegistry#getComponent(TenantDescriptor, Class)} to create
     * a type-specific instance. This is used when the handler parameter is a subtype of
     * the registered component type.
     *
     * @param registry       the registry for tenant-scoped components
     * @param tenantResolver the resolver used to determine which tenant a message belongs to
     * @param requestedType  the specific subtype requested, or {@code null} for exact matching
     */
    public TenantComponentResolver(TenantComponentRegistry<T> registry,
                                   TargetTenantResolver<Message> tenantResolver,
                                   @Nullable Class<? extends T> requestedType) {
        this.registry = Objects.requireNonNull(registry, "TenantComponentRegistry must not be null");
        this.tenantResolver = Objects.requireNonNull(tenantResolver, "TargetTenantResolver must not be null");
        this.requestedType = requestedType;
    }

    @Override
    public CompletableFuture<T> resolveParameterValue(ProcessingContext context) {
        Message message = Message.fromContext(context);
        if (message == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No message found in ProcessingContext"));
        }

        try {
            TenantDescriptor tenant = tenantResolver.resolveTenant(message, registry.getTenants());
            T component = requestedType != null
                    ? registry.getComponent(tenant, requestedType)
                    : registry.getComponent(tenant);
            return CompletableFuture.completedFuture(component);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public boolean matches(ProcessingContext context) {
        Message message = Message.fromContext(context);
        if (message == null) {
            return false;
        }
        try {
            TenantDescriptor tenant = tenantResolver.resolveTenant(message, registry.getTenants());
            return registry.getTenants().contains(tenant);
        } catch (Exception e) {
            return false;
        }
    }
}
