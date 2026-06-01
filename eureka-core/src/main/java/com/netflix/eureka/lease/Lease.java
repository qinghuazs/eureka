/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.eureka.lease;

import com.netflix.eureka.registry.AbstractInstanceRegistry;

/**
 * Describes a time-based availability of a {@link T}. Purpose is to avoid
 * accumulation of instances in {@link AbstractInstanceRegistry} as result of ungraceful
 * shutdowns that is not uncommon in AWS environments.
 *
 * If a lease elapses without renewals, it will eventually expire consequently
 * marking the associated {@link T} for immediate eviction - this is similar to
 * an explicit cancellation except that there is no communication between the
 * {@link T} and {@link LeaseManager}.
 *
 * @author Karthik Ranganathan, Greg Kim
 */
// 【租约模型】注册表中存储的不是裸的 InstanceInfo，而是包了一层 Lease（租约）。
// 租约记录了实例的生命周期时间戳：注册时间、最近续约时间、下线时间、服务上线时间。
// 设计目的：AWS 环境中实例非优雅下线（直接被销毁）很常见，通过"租约+心跳续约"机制，
// 让长时间未续约的实例自动过期被剔除，避免注册表中堆积僵尸实例。
public class Lease<T> {

    enum Action {
        Register, Cancel, Renew
    };

    // 默认租约时长 90 秒：超过 90s 未收到心跳，租约过期（实际因下方 renew() 的 bug，是 180s）
    public static final int DEFAULT_DURATION_IN_SECS = 90;

    // 租约持有者，实际类型就是 InstanceInfo
    private T holder;
    // 下线时间戳：调用 cancel() 主动下线时设置
    private long evictionTimestamp;
    // 注册时间戳
    private long registrationTimestamp;
    // 服务上线（状态变为 UP）时间戳
    private long serviceUpTimestamp;
    // Make it volatile so that the expiration task would see this quicker
    // 最近续约时间戳（volatile 保证剔除任务能尽快看到最新值）
    private volatile long lastUpdateTimestamp;
    // 租约时长（毫秒）
    private long duration;

    public Lease(T r, int durationInSecs) {
        holder = r;
        registrationTimestamp = System.currentTimeMillis();
        lastUpdateTimestamp = registrationTimestamp;
        duration = (durationInSecs * 1000);

    }

    /**
     * Renew the lease, use renewal duration if it was specified by the
     * associated {@link T} during registration, otherwise default duration is
     * {@link #DEFAULT_DURATION_IN_SECS}.
     */
    // 【续约】更新最近续约时间戳。
    // 注意这里有个著名的"bug"：正确写法应该是 lastUpdateTimestamp = 当前时间，
    // 但这里多加了一个 duration，导致实际过期判定时间 = 2 倍 duration（180s 而非 90s）。
    // Netflix 官方明确表示不修复（见 isExpired 的 javadoc），因为影响面太大
    public void renew() {
        lastUpdateTimestamp = System.currentTimeMillis() + duration;

    }

    /**
     * Cancels the lease by updating the eviction time.
     */
    // 【下线】记录下线时间戳（只在第一次调用时生效），isExpired() 会因此立即返回 true
    public void cancel() {
        if (evictionTimestamp <= 0) {
            evictionTimestamp = System.currentTimeMillis();
        }
    }

    /**
     * Mark the service as up. This will only take affect the first time called,
     * subsequent calls will be ignored.
     */
    public void serviceUp() {
        if (serviceUpTimestamp == 0) {
            serviceUpTimestamp = System.currentTimeMillis();
        }
    }

    /**
     * Set the leases service UP timestamp.
     */
    public void setServiceUpTimestamp(long serviceUpTimestamp) {
        this.serviceUpTimestamp = serviceUpTimestamp;
    }

    /**
     * Checks if the lease of a given {@link com.netflix.appinfo.InstanceInfo} has expired or not.
     */
    public boolean isExpired() {
        return isExpired(0l);
    }

    /**
     * Checks if the lease of a given {@link com.netflix.appinfo.InstanceInfo} has expired or not.
     *
     * Note that due to renew() doing the 'wrong" thing and setting lastUpdateTimestamp to +duration more than
     * what it should be, the expiry will actually be 2 * duration. This is a minor bug and should only affect
     * instances that ungracefully shutdown. Due to possible wide ranging impact to existing usage, this will
     * not be fixed.
     *
     * @param additionalLeaseMs any additional lease time to add to the lease evaluation in ms.
     */
    // 【过期判定】剔除任务（EvictionTask）调用此方法判断实例是否该被剔除。
    // 过期条件（满足其一）：
    //   1. 实例已主动下线（evictionTimestamp > 0）
    //   2. 当前时间 > 最近续约时间 + 租约时长 + 补偿时间
    //      （补偿时间 additionalLeaseMs 用于修正 GC 暂停/时钟偏移导致的剔除任务延迟执行）
    // 注意：由于 renew() 的 bug，实际过期时间是注释所说的 2 倍 duration（默认 180s）
    public boolean isExpired(long additionalLeaseMs) {
        return (evictionTimestamp > 0 || System.currentTimeMillis() > (lastUpdateTimestamp + duration + additionalLeaseMs));
    }

    /**
     * Gets the milliseconds since epoch when the lease was registered.
     *
     * @return the milliseconds since epoch when the lease was registered.
     */
    public long getRegistrationTimestamp() {
        return registrationTimestamp;
    }

    /**
     * Gets the milliseconds since epoch when the lease was last renewed.
     * Note that the value returned here is actually not the last lease renewal time but the renewal + duration.
     *
     * @return the milliseconds since epoch when the lease was last renewed.
     */
    public long getLastRenewalTimestamp() {
        return lastUpdateTimestamp;
    }

    /**
     * Gets the milliseconds since epoch when the lease was evicted.
     *
     * @return the milliseconds since epoch when the lease was evicted.
     */
    public long getEvictionTimestamp() {
        return evictionTimestamp;
    }

    /**
     * Gets the milliseconds since epoch when the service for the lease was marked as up.
     *
     * @return the milliseconds since epoch when the service for the lease was marked as up.
     */
    public long getServiceUpTimestamp() {
        return serviceUpTimestamp;
    }

    /**
     * Returns the holder of the lease.
     */
    public T getHolder() {
        return holder;
    }

}
