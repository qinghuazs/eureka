/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.shared.transport.decorator;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;

/**
 * 【装饰器链基座 —— 对应文档06】
 *
 * 这是 Eureka 客户端传输层【所有装饰器】(Retryable / Sessioned / Redirecting) 的公共抽象父类，
 * 同时它本身实现了 {@link EurekaHttpClient}（与被装饰对象同一接口），这是装饰器模式的标准特征：
 * 装饰器与被装饰者面向同一接口，因此可以层层包裹而对调用方透明。
 *
 * 设计要点（一句话）：把"装饰逻辑(重试/会话/重定向)"与"具体操作(注册/心跳/拉取...)"彻底解耦。
 *   - {@link EurekaHttpClient} 接口里有十几个方法 (register/cancel/sendHeartBeat/getApplications...)，
 *     如果每个子类都为每个方法各写一遍重试/会话/重定向逻辑，将是十几倍的重复代码、且极易遗漏。
 *   - 本基座用【模板方法】把这套"包装-委派"流程统一收口到唯一的抽象方法 {@link #execute(RequestExecutor)}：
 *     本类把每一个接口方法都实现成"将该次调用打包成一个 {@link RequestExecutor} 回调，再交给 execute() 执行"。
 *   - 于是子类只需重写【一个】 execute() 方法，就能对【全部】操作统一附加自己的横切能力。
 *     这正是装饰器模式与模板方法模式结合后的核心收益。
 *
 * @author Tomasz Bak
 */
public abstract class EurekaHttpClientDecorator implements EurekaHttpClient {

    /**
     * 操作类型枚举：穷举 {@link EurekaHttpClient} 对外暴露的全部请求种类。
     *
     * 它的价值在于：装饰器在 execute() 里拿到的是一个"被打包的请求回调"，并不知道具体调用的是哪个方法；
     * 通过 {@link RequestExecutor#getRequestType()} 返回的这个枚举，子类就能在运行时区分请求类型，
     * 从而做出差异化决策。最典型的用途见 RedirectingEurekaHttpClient / RetryableEurekaHttpClient：
     * 例如只对"写操作 / 注册类"做某种重定向或重试处理，对"读操作"区别对待。
     */
    public enum RequestType {
        Register,               // 注册：把本实例信息上报给 Server（POST /apps/{appName}）
        Cancel,                 // 下线：通知 Server 主动剔除本实例（DELETE /apps/{appName}/{id}）
        SendHeartBeat,          // 心跳续约：周期性告知 Server "我还活着"（PUT /apps/{appName}/{id}）
        StatusUpdate,           // 状态覆盖：手工设置实例状态 OVERRIDE（PUT .../status）
        DeleteStatusOverride,   // 删除状态覆盖：撤销上面的手工状态覆盖（DELETE .../status）
        GetApplications,        // 全量拉取：获取注册表里所有应用（GET /apps）
        GetDelta,               // 增量拉取：只获取自上次以来的变更（GET /apps/delta），客户端定时刷新的关键
        GetVip,                 // 按 VIP 地址拉取应用列表
        GetSecureVip,           // 按安全(Secure) VIP 地址拉取应用列表
        GetApplication,         // 按 appName 拉取单个应用
        GetInstance,            // 按实例 id 拉取单个实例
        GetApplicationInstance  // 按 appName + id 精确拉取某应用下的某实例
    }

    /**
     * 请求执行器：把"对某个底层 EurekaHttpClient 发起的一次具体调用"抽象成一个可重复执行的回调对象。
     *
     * 这是整个基座的【关键抽象】。本类下面的每个接口方法（register/sendHeartBeat/...）都会 new 一个匿名
     * RequestExecutor：在它的 {@link #execute(EurekaHttpClient)} 里写明"拿到一个 delegate 后到底要调用它的哪个方法、
     * 传哪些参数"，并把这些参数用 final 闭包捕获。如此一来，原本"调用哪个方法"这件本来固定死的事，被延迟、被参数化了。
     *
     * 装饰器拿到这个回调后，就可以反复 / 换不同的 delegate 去 execute 它，从而实现重试、换节点、跟随重定向等能力，
     * 而完全不必关心底层到底是注册还是心跳。
     *
     * @param <R> 该次请求返回体的类型（Void / InstanceInfo / Applications / Application 等）
     */
    public interface RequestExecutor<R> {
        // 真正发起调用：由装饰器传入一个具体的底层客户端 delegate，回调内部用它执行那一次确定的请求
        EurekaHttpResponse<R> execute(EurekaHttpClient delegate);

        // 暴露本次请求的类型，供装饰器按 RequestType 做差异化处理（见上方枚举注释）
        RequestType getRequestType();
    }

    /**
     * 【模板方法】—— 整个基座唯一的抽象方法，也是子类唯一需要实现的方法。
     *
     * 本类把所有接口方法都转化为"构造 RequestExecutor 回调 → 调用 execute() 执行"的统一形态，
     * 因此具体的横切逻辑全部集中到这里：
     *   - RetryableEurekaHttpClient：在这里实现"失败后换一个 Server 节点重试 N 次"；
     *   - SessionedEurekaHttpClient：在这里实现"会话到期则重建底层连接/客户端"；
     *   - RedirectingEurekaHttpClient：在这里实现"跟随 302 重定向并记忆目标节点"。
     * 子类只重写这一个方法，就自动对【全部】操作类型生效——这正是模板方法 + 装饰器组合的精髓。
     */
    protected abstract <R> EurekaHttpResponse<R> execute(RequestExecutor<R> requestExecutor);

    /*
     * 注册操作。下面这段是本基座所有接口方法的【统一范式】，后续各方法都同此结构，不再逐一展开：
     *   1) new 一个匿名 RequestExecutor，用 final 参数（这里是 info）闭包捕获本次调用所需的入参；
     *   2) 在回调的 execute(delegate) 里，写明"对底层客户端要调用的正是同名方法 delegate.register(info)"；
     *   3) 在 getRequestType() 里声明本次请求类型为 RequestType.Register；
     *   4) 把这个回调交给模板方法 execute() —— 重试/会话/重定向等横切能力就在那里被统一施加。
     */
    @Override
    public EurekaHttpResponse<Void> register(final InstanceInfo info) {
        return execute(new RequestExecutor<Void>() {
            @Override
            public EurekaHttpResponse<Void> execute(EurekaHttpClient delegate) {
                return delegate.register(info);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.Register;
            }
        });
    }

    // 下线操作：同上范式，打包成 RequestType.Cancel 的回调交给模板方法
    @Override
    public EurekaHttpResponse<Void> cancel(final String appName, final String id) {
        return execute(new RequestExecutor<Void>() {
            @Override
            public EurekaHttpResponse<Void> execute(EurekaHttpClient delegate) {
                return delegate.cancel(appName, id);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.Cancel;
            }
        });
    }

    // 心跳续约：客户端定时调用的高频操作，打包成 RequestType.SendHeartBeat 的回调交给模板方法
    @Override
    public EurekaHttpResponse<InstanceInfo> sendHeartBeat(final String appName,
                                                          final String id,
                                                          final InstanceInfo info,
                                                          final InstanceStatus overriddenStatus) {
        return execute(new RequestExecutor<InstanceInfo>() {
            @Override
            public EurekaHttpResponse<InstanceInfo> execute(EurekaHttpClient delegate) {
                return delegate.sendHeartBeat(appName, id, info, overriddenStatus);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.SendHeartBeat;
            }
        });
    }

    // 状态覆盖：手工设置实例状态，打包成 RequestType.StatusUpdate 的回调交给模板方法
    @Override
    public EurekaHttpResponse<Void> statusUpdate(final String appName, final String id, final InstanceStatus newStatus, final InstanceInfo info) {
        return execute(new RequestExecutor<Void>() {
            @Override
            public EurekaHttpResponse<Void> execute(EurekaHttpClient delegate) {
                return delegate.statusUpdate(appName, id, newStatus, info);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.StatusUpdate;
            }
        });
    }

    // 删除状态覆盖：撤销手工状态覆盖，打包成 RequestType.DeleteStatusOverride 的回调交给模板方法
    @Override
    public EurekaHttpResponse<Void> deleteStatusOverride(final String appName, final String id, final InstanceInfo info) {
        return execute(new RequestExecutor<Void>() {
            @Override
            public EurekaHttpResponse<Void> execute(EurekaHttpClient delegate) {
                return delegate.deleteStatusOverride(appName, id, info);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.DeleteStatusOverride;
            }
        });
    }

    // 全量拉取：获取全部应用，打包成 RequestType.GetApplications 的回调交给模板方法
    @Override
    public EurekaHttpResponse<Applications> getApplications(final String... regions) {
        return execute(new RequestExecutor<Applications>() {
            @Override
            public EurekaHttpResponse<Applications> execute(EurekaHttpClient delegate) {
                return delegate.getApplications(regions);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.GetApplications;
            }
        });
    }

    // 增量拉取：只取变更部分，打包成 RequestType.GetDelta 的回调交给模板方法（客户端定时刷新注册表的关键路径）
    @Override
    public EurekaHttpResponse<Applications> getDelta(final String... regions) {
        return execute(new RequestExecutor<Applications>() {
            @Override
            public EurekaHttpResponse<Applications> execute(EurekaHttpClient delegate) {
                return delegate.getDelta(regions);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.GetDelta;
            }
        });
    }

    // 按 VIP 拉取：打包成 RequestType.GetVip 的回调交给模板方法
    @Override
    public EurekaHttpResponse<Applications> getVip(final String vipAddress, final String... regions) {
        return execute(new RequestExecutor<Applications>() {
            @Override
            public EurekaHttpResponse<Applications> execute(EurekaHttpClient delegate) {
                return delegate.getVip(vipAddress, regions);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.GetVip;
            }
        });
    }

    // 按安全 VIP 拉取：打包成 RequestType.GetSecureVip 的回调交给模板方法
    @Override
    public EurekaHttpResponse<Applications> getSecureVip(final String secureVipAddress, final String... regions) {
        return execute(new RequestExecutor<Applications>() {
            @Override
            public EurekaHttpResponse<Applications> execute(EurekaHttpClient delegate) {
                // 注意：此处回调内调用的是 delegate.getVip(...)（与方法名 getSecureVip 略有出入），属源码现状，仅作客观说明、不作改动
                return delegate.getVip(secureVipAddress, regions);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.GetSecureVip;
            }
        });
    }

    // 按 appName 拉取单个应用：打包成 RequestType.GetApplication 的回调交给模板方法
    @Override
    public EurekaHttpResponse<Application> getApplication(final String appName) {
        return execute(new RequestExecutor<Application>() {
            @Override
            public EurekaHttpResponse<Application> execute(EurekaHttpClient delegate) {
                return delegate.getApplication(appName);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.GetApplication;
            }
        });
    }

    // 按实例 id 拉取（重载一）：打包成 RequestType.GetInstance 的回调交给模板方法
    @Override
    public EurekaHttpResponse<InstanceInfo> getInstance(final String id) {
        return execute(new RequestExecutor<InstanceInfo>() {
            @Override
            public EurekaHttpResponse<InstanceInfo> execute(EurekaHttpClient delegate) {
                return delegate.getInstance(id);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.GetInstance;
            }
        });
    }

    // 按 appName + id 精确拉取（重载二）：注意请求类型是 RequestType.GetApplicationInstance（区别于上面的 GetInstance）
    @Override
    public EurekaHttpResponse<InstanceInfo> getInstance(final String appName, final String id) {
        return execute(new RequestExecutor<InstanceInfo>() {
            @Override
            public EurekaHttpResponse<InstanceInfo> execute(EurekaHttpClient delegate) {
                return delegate.getInstance(appName, id);
            }

            @Override
            public RequestType getRequestType() {
                return RequestType.GetApplicationInstance;
            }
        });
    }
}
