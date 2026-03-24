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

import org.axonframework.common.Priority;
import org.axonframework.extension.multitenancy.common.TargetTenantResolver;
import org.axonframework.extension.multitenancy.common.TenantComponentFactory;
import org.axonframework.extension.multitenancy.common.TenantComponentRegistry;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link ParameterResolverFactory} that creates resolvers for tenant-scoped components.
 * <p>
 * This factory manages multiple {@link TenantComponentRegistry} instances, one for each
 * registered component type. When a handler method has a parameter matching a registered
 * component type, this factory creates a {@link TenantComponentResolver} to provide
 * the tenant-scoped instance.
 * <p>
 * This factory runs with {@link Priority#HIGH} to ensure it processes component parameters
 * before other resolvers that might attempt to inject non-tenant-aware instances.
 * <p>
 * Example registration and usage:
 * <pre>{@code
 * // Registration via ComponentRegistry
 * configurer.componentRegistry(cr ->
 *     cr.registerComponent(OrderRepository.class, config -> new InMemoryOrderRepository())
 * );
 *
 * // Handler receives tenant-scoped instance
 * @EventHandler
 * public void on(OrderCreatedEvent event, OrderRepository repository) {
 *     repository.save(new OrderProjection(event));
 * }
 * }</pre>
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see TenantComponentResolver
 * @see TenantComponentRegistry
 */
@Priority(Priority.HIGH)
public class TenantComponentResolverFactory implements ParameterResolverFactory {

    private final Map<Class<?>, TenantComponentRegistry<?>> registries = new ConcurrentHashMap<>();
    private final TargetTenantResolver<Message> tenantResolver;

    /**
     * Creates a new {@link TenantComponentResolverFactory}.
     *
     * @param tenantResolver the resolver used to determine which tenant a message belongs to
     */
    public TenantComponentResolverFactory(TargetTenantResolver<Message> tenantResolver) {
        this.tenantResolver = Objects.requireNonNull(tenantResolver, "TargetTenantResolver must not be null");
    }

    /**
     * Registers a component type with its factory.
     * <p>
     * This creates a {@link TenantComponentRegistry} for the component type that will
     * lazily create instances using the provided factory when first accessed.
     *
     * @param componentType the class of the component
     * @param factory       the factory to create component instances per tenant
     * @param <T>           the component type
     * @return the created registry for lifecycle management
     */
    public <T> TenantComponentRegistry<T> registerComponent(Class<T> componentType,
                                                            TenantComponentFactory<T> factory) {
        TenantComponentRegistry<T> registry = new TenantComponentRegistry<>(componentType, factory);
        registries.put(componentType, registry);
        return registry;
    }

    /**
     * Returns the registries managed by this factory.
     * <p>
     * This is used for lifecycle management, allowing the registries to be
     * subscribed to tenant providers.
     *
     * @return an unmodifiable view of the registered component registries
     */
    public Map<Class<?>, TenantComponentRegistry<?>> getRegistries() {
        return Map.copyOf(registries);
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public ParameterResolver<?> createInstance(Executable executable,
                                               Parameter[] parameters,
                                               int parameterIndex) {
        Class<?> parameterType = parameters[parameterIndex].getType();

        // Exact type match (most common case)
        TenantComponentRegistry<?> registry = registries.get(parameterType);
        if (registry != null) {
            return new TenantComponentResolver<>(
                    (TenantComponentRegistry<Object>) registry,
                    tenantResolver
            );
        }

        // Assignability fallback: find a registry whose component type is a supertype
        // of the requested parameter type. This allows a factory registered for a base
        // type (e.g., Repository) to serve any subtype (e.g., OrderRepository).
        for (var entry : registries.entrySet()) {
            if (entry.getKey().isAssignableFrom(parameterType)) {
                return new TenantComponentResolver<>(
                        (TenantComponentRegistry<Object>) entry.getValue(),
                        tenantResolver,
                        (Class<Object>) parameterType
                );
            }
        }

        return null;
    }
}
