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

import org.axonframework.common.Registration;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that manages tenant-scoped component instances.
 * <p>
 * This registry caches component instances per tenant, creating them lazily
 * on first access using the provided {@link TenantComponentFactory}. When a
 * tenant is unregistered, its component instance is removed from the cache
 * and cleaned up via {@link TenantComponentFactory#destroy(TenantDescriptor, Object)}.
 * <p>
 * The registry implements {@link MultiTenantAwareComponent} to participate
 * in tenant lifecycle management, but uses lazy creation - components are
 * only instantiated when first requested, not when a tenant is registered.
 * <p>
 * Components that implement {@link AutoCloseable} are automatically closed
 * when their tenant is removed (unless custom cleanup is provided via the factory).
 *
 * @param <T> the type of component managed by this registry
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see TenantComponentFactory
 */
public class TenantComponentRegistry<T> implements MultiTenantAwareComponent {

    private final Class<T> componentType;
    private final TenantComponentFactory<T> factory;
    private final ConcurrentHashMap<TenantDescriptor, T> components = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TenantDescriptor, ConcurrentHashMap<Class<?>, T>> typedComponents =
            new ConcurrentHashMap<>();
    private final Set<TenantDescriptor> registeredTenants = ConcurrentHashMap.newKeySet();

    /**
     * Creates a new registry for the given component type and factory.
     *
     * @param componentType the class of the component type for parameter matching
     * @param factory       the factory to create component instances per tenant
     */
    public TenantComponentRegistry(Class<T> componentType,
                                   TenantComponentFactory<T> factory) {
        this.componentType = Objects.requireNonNull(componentType, "Component type must not be null");
        this.factory = Objects.requireNonNull(factory, "Factory must not be null");
    }

    /**
     * Gets the component instance for the given tenant, creating it if necessary.
     * <p>
     * Components are created lazily using the configured factory. Once created,
     * they are cached for subsequent calls with the same tenant.
     *
     * @param tenant the tenant descriptor
     * @return the component instance for this tenant
     */
    public T getComponent(TenantDescriptor tenant) {
        return components.computeIfAbsent(tenant, factory);
    }

    /**
     * Gets a component instance of the given {@code requestedType} for the given tenant.
     * <p>
     * This is used when a handler parameter's type is a subtype of this registry's
     * component type. The factory's {@link TenantComponentFactory#create(TenantDescriptor, Class)}
     * method is called with the requested subtype. Components are cached per (tenant, type) pair.
     *
     * @param tenant        the tenant descriptor
     * @param requestedType the specific subtype requested
     * @param <S>           the subtype
     * @return the component instance for this tenant and type
     */
    @SuppressWarnings("unchecked")
    public <S extends T> S getComponent(TenantDescriptor tenant, Class<S> requestedType) {
        return (S) typedComponents
                .computeIfAbsent(tenant, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(requestedType, type -> factory.create(tenant, (Class<T>) type));
    }

    /**
     * Returns the component type managed by this registry.
     *
     * @return the component class
     */
    public Class<T> getComponentType() {
        return componentType;
    }

    /**
     * Returns the set of registered tenants.
     * <p>
     * Note that this returns all tenants that have been registered, not just
     * those for which components have been created. Use this for tenant resolution.
     *
     * @return an unmodifiable view of registered tenant descriptors
     */
    public Set<TenantDescriptor> getTenants() {
        return Set.copyOf(registeredTenants);
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        registeredTenants.add(tenantDescriptor);
        // Lazy creation - don't create component until first access
        return () -> {
            boolean wasRegistered = registeredTenants.remove(tenantDescriptor);
            // Collect all unique component instances from both caches to avoid double-destroy
            Set<T> destroyed = Collections.newSetFromMap(new IdentityHashMap<>());
            T removed = components.remove(tenantDescriptor);
            if (removed != null) {
                destroyed.add(removed);
            }
            ConcurrentHashMap<Class<?>, T> typedForTenant = typedComponents.remove(tenantDescriptor);
            if (typedForTenant != null) {
                destroyed.addAll(typedForTenant.values());
            }
            destroyed.forEach(component -> factory.destroy(tenantDescriptor, component));
            return wasRegistered;
        };
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        // For component registries, there's nothing to "start"
        return registerTenant(tenantDescriptor);
    }
}
