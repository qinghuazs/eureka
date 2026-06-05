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

package com.netflix.appinfo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

/**
 * Represents the <em>lease</em> information with <em>Eureka</em>.
 *
 * <p>
 * <em>Eureka</em> decides to remove the instance out of its view depending on
 * the duration that is set in
 * {@link EurekaInstanceConfig#getLeaseExpirationDurationInSeconds()} which is
 * held in this lease. The lease also tracks the last time it was renewed.
 * </p>
 *
 * @author Karthik Ranganathan, Greg Kim
 *
 */
/*
 * 【中文导读 · 对应文档09(三大数据模型之一)】
 * LeaseInfo 是租约信息在「客户端侧」的数据载体，作为 InstanceInfo 的一个字段随注册请求一起上报到服务端。
 * 它包含两类信息：
 *   1) Client settings(客户端声明的续约参数)：renewalIntervalInSecs / durationInSecs，由客户端配置后随注册发给服务端，
 *      告诉服务端"我多久续约一次、超过多久没续约就可以认为我挂了"。
 *   2) Server populated(服务端填充的时间戳)：registration / lastRenewal / eviction / serviceUp 等，
 *      由服务端在注册、续约、剔除等动作发生时回写。
 *
 * 重要对照：本类与服务端的 com.netflix.eureka.lease.Lease(已注释)是一对"声明 ↔ 判定"的关系——
 *   · LeaseInfo 是客户端「声明」的续约参数(意图/契约)；
 *   · 服务端 Lease 则据此持有 duration 并实现 isExpired()，依据 lastUpdateTimestamp 与 duration 判断租约是否过期。
 * 也就是说：客户端在 LeaseInfo 里说"我承诺这个续约节奏"，服务端 Lease 拿着这个承诺去做过期裁决与剔除(evict)。
 */
@JsonRootName("leaseInfo")
public class LeaseInfo {

    // 续约间隔默认值：30 秒。即客户端默认每 30s 向服务端发送一次心跳(续约)。
    public static final int DEFAULT_LEASE_RENEWAL_INTERVAL = 30;
    // 租约时长默认值：90 秒。即服务端默认在最近一次续约后 90s 内未再收到续约，就认为该租约过期。
    // 默认 90s 恰为续约间隔 30s 的 3 倍，留出容忍若干次心跳丢失的余量。
    public static final int DEFAULT_LEASE_DURATION = 90;

    // ===== Client settings：客户端声明的续约参数，随注册请求上报，服务端据此判过期 =====
    // 续约间隔(秒)：客户端两次心跳之间的间隔，默认 30s。
    private int renewalIntervalInSecs = DEFAULT_LEASE_RENEWAL_INTERVAL;
    // 租约时长(秒)：服务端允许的"最长无续约时间"，超过即视为过期，默认 90s。对应服务端 Lease 的 duration。
    private int durationInSecs = DEFAULT_LEASE_DURATION;

    // ===== Server populated：以下时间戳由服务端在相应动作发生时填充 =====
    // 注册时间戳：该租约首次注册到服务端的时刻(epoch 毫秒)。
    private long registrationTimestamp;
    // 最近一次续约时间戳：服务端最后一次收到该实例心跳的时刻；服务端用它 + durationInSecs 判定是否过期。
    private long lastRenewalTimestamp;
    // 剔除时间戳：该租约被服务端下线/剔除(主动注销或过期被 evict)的时刻。
    private long evictionTimestamp;
    // 服务上线时间戳：该实例状态被标记为 UP(对外可用)的时刻。
    private long serviceUpTimestamp;

    public static final class Builder {

        @XStreamOmitField
        private LeaseInfo result;

        private Builder() {
            result = new LeaseInfo();
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Sets the registration timestamp.
         *
         * @param ts
         *            time when the lease was first registered.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setRegistrationTimestamp(long ts) {
            result.registrationTimestamp = ts;
            return this;
        }

        /**
         * Sets the last renewal timestamp of lease.
         *
         * @param ts
         *            time when the lease was last renewed.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setRenewalTimestamp(long ts) {
            result.lastRenewalTimestamp = ts;
            return this;
        }

        /**
         * Sets the de-registration timestamp.
         *
         * @param ts
         *            time when the lease was removed.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setEvictionTimestamp(long ts) {
            result.evictionTimestamp = ts;
            return this;
        }

        /**
         * Sets the service UP timestamp.
         *
         * @param ts
         *            time when the leased service marked as UP.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setServiceUpTimestamp(long ts) {
            result.serviceUpTimestamp = ts;
            return this;
        }

        /**
         * Sets the client specified setting for eviction (e.g. how long to wait
         * without renewal event).
         *
         * @param d
         *            time in seconds after which the lease would expire without
         *            renewa.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setDurationInSecs(int d) {
            if (d <= 0) {
                result.durationInSecs = DEFAULT_LEASE_DURATION;
            } else {
                result.durationInSecs = d;
            }
            return this;
        }

        /**
         * Sets the client specified setting for renew interval.
         *
         * @param i
         *            the time interval with which the renewals will be renewed.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setRenewalIntervalInSecs(int i) {
            if (i <= 0) {
                result.renewalIntervalInSecs = DEFAULT_LEASE_RENEWAL_INTERVAL;
            } else {
                result.renewalIntervalInSecs = i;
            }
            return this;
        }

        /**
         * Build the {@link InstanceInfo}.
         *
         * @return the {@link LeaseInfo} information built based on the supplied
         *         information.
         */
        public LeaseInfo build() {
            return result;
        }
    }

    private LeaseInfo() {
    }

    /**
     * TODO: note about renewalTimestamp legacy:
     * The previous change to use Jackson ser/deser changed the field name for lastRenewalTimestamp to renewalTimestamp
     * for serialization, which causes an incompatibility with the jacksonNG codec when the server returns data with
     * field renewalTimestamp and jacksonNG expects lastRenewalTimestamp. Remove this legacy field from client code
     * in a few releases (once servers are updated to a release that generates json with the correct
     * lastRenewalTimestamp).
     */
    @JsonCreator
    public LeaseInfo(@JsonProperty("renewalIntervalInSecs") int renewalIntervalInSecs,
                     @JsonProperty("durationInSecs") int durationInSecs,
                     @JsonProperty("registrationTimestamp") long registrationTimestamp,
                     @JsonProperty("lastRenewalTimestamp") Long lastRenewalTimestamp,
                     @JsonProperty("renewalTimestamp") long lastRenewalTimestampLegacy,  // for legacy
                     @JsonProperty("evictionTimestamp") long evictionTimestamp,
                     @JsonProperty("serviceUpTimestamp") long serviceUpTimestamp) {
        this.renewalIntervalInSecs = renewalIntervalInSecs;
        this.durationInSecs = durationInSecs;
        this.registrationTimestamp = registrationTimestamp;
        this.evictionTimestamp = evictionTimestamp;
        this.serviceUpTimestamp = serviceUpTimestamp;

        if (lastRenewalTimestamp == null) {
            this.lastRenewalTimestamp = lastRenewalTimestampLegacy;
        } else {
            this.lastRenewalTimestamp = lastRenewalTimestamp;
        }
    }

    /**
     * Returns the registration timestamp.
     *
     * @return time in milliseconds since epoch.
     */
    public long getRegistrationTimestamp() {
        return registrationTimestamp;
    }

    /**
     * Returns the last renewal timestamp of lease.
     *
     * @return time in milliseconds since epoch.
     */
    @JsonProperty("lastRenewalTimestamp")
    public long getRenewalTimestamp() {
        return lastRenewalTimestamp;
    }

    /**
     * Returns the de-registration timestamp.
     *
     * @return time in milliseconds since epoch.
     */
    public long getEvictionTimestamp() {
        return evictionTimestamp;
    }

    /**
     * Returns the service UP timestamp.
     *
     * @return time in milliseconds since epoch.
     */
    public long getServiceUpTimestamp() {
        return serviceUpTimestamp;
    }

    /**
     * Returns client specified setting for renew interval.
     *
     * @return time in milliseconds since epoch.
     */
    public int getRenewalIntervalInSecs() {
        return renewalIntervalInSecs;
    }

    /**
     * Returns client specified setting for eviction (e.g. how long to wait w/o
     * renewal event)
     *
     * @return time in milliseconds since epoch.
     */
    public int getDurationInSecs() {
        return durationInSecs;
    }

}
