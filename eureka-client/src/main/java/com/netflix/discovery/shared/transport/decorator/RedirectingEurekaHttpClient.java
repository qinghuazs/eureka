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

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.discovery.shared.dns.DnsService;
import com.netflix.discovery.shared.dns.DnsServiceImpl;
import com.netflix.discovery.shared.resolver.DefaultEndpoint;
import com.netflix.discovery.shared.resolver.EurekaEndpoint;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import com.netflix.discovery.shared.transport.TransportClientFactory;
import com.netflix.discovery.shared.transport.TransportException;
import com.netflix.discovery.shared.transport.TransportUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link EurekaHttpClient} that follows redirect links, and executes the requests against
 * the finally resolved endpoint.
 * If registration and query requests must handled separately, two different instances shall be created.
 * <h3>Thread safety</h3>
 * Methods in this class may be called concurrently.
 *
 * @author Tomasz Bak
 */
public class RedirectingEurekaHttpClient extends EurekaHttpClientDecorator {

    private static final Logger logger = LoggerFactory.getLogger(RedirectingEurekaHttpClient.class);

    // 单次请求最多跟随 302 跳转的次数上限：超过 10 次仍未命中真实处理节点，就判定为链路异常并直接抛错，
    // 避免被错误配置或恶意 Location 头拖入无限重定向死循环。对应文档06"重定向层"。
    public static final int MAX_FOLLOWED_REDIRECTS = 10;
    // 用于从重定向回来的 Location 中识别出 Eureka 标准路径（.../v2/apps...），并截取出可复用的 base 段。
    // group(1) 即 "scheme://host:port/.../v2/" 这一前缀，后续会把它当作直连目标节点的服务地址。
    private static final Pattern REDIRECT_PATH_REGEX = Pattern.compile("(.*/v2/)apps(/.*)?$");

    private final EurekaEndpoint serviceEndpoint;
    private final TransportClientFactory factory;
    private final DnsService dnsService;

    // 重定向的"粘性"就缓存在这里：一旦把请求解析到真正处理它的 Server，就把指向该 Server 的 client 存进 delegateRef。
    // 此后请求直接走这个缓存的 delegate、不再每次从入口重定向，省掉一次 302 跳转（文档06的关键论断）。
    // 用 AtomicReference 是因为本类被多线程并发调用，缓存的写入/替换需要原子语义。
    private final AtomicReference<EurekaHttpClient> delegateRef = new AtomicReference<>();

    /**
     * The delegate client should pass through 3xx responses without further processing.
     */
    public RedirectingEurekaHttpClient(String serviceUrl, TransportClientFactory factory, DnsService dnsService) {
        this.serviceEndpoint = new DefaultEndpoint(serviceUrl);
        this.factory = factory;
        this.dnsService = dnsService;
    }

    @Override
    public void shutdown() {
        TransportUtils.shutdown(delegateRef.getAndSet(null));
    }

    /*
     * 重定向层的核心入口。逻辑分两条路：
     *   1) delegate 还没被解析出来（首次请求或上次失败被清空）——走 executeOnNewServer 跟随重定向，
     *      解析成功后把结果节点缓存进 delegateRef，让后续请求享受"粘性"直连。
     *   2) delegate 已缓存——直接拿它执行，跳过整套重定向流程；一旦出错就把缓存清空，
     *      下次请求重新从入口节点解析（容错：处理节点可能已下线/迁移）。
     */
    @Override
    protected <R> EurekaHttpResponse<R> execute(RequestExecutor<R> requestExecutor) {
        EurekaHttpClient currentEurekaClient = delegateRef.get();
        if (currentEurekaClient == null) {
            // 还没解析出真实节点：从入口 serviceEndpoint 起一个新 client，跟随重定向直到命中处理节点
            AtomicReference<EurekaHttpClient> currentEurekaClientRef = new AtomicReference<>(factory.newClient(serviceEndpoint));
            try {
                EurekaHttpResponse<R> response = executeOnNewServer(requestExecutor, currentEurekaClientRef);
                // 解析成功：把指向真实处理节点的 client 原子地设进 delegateRef（建立"粘性"），
                // getAndSet 返回的旧值若非空则一并关闭，避免连接泄漏。
                TransportUtils.shutdown(delegateRef.getAndSet(currentEurekaClientRef.get()));
                return response;
            } catch (Exception e) {
                logger.info("Request execution error. endpoint={}, exception={} stacktrace={}", serviceEndpoint,
                        e.getMessage(), ExceptionUtils.getStackTrace(e));
                // 解析过程出错：本次临时 client 不能缓存，直接关闭丢弃，下次请求重新从入口开始解析
                TransportUtils.shutdown(currentEurekaClientRef.get());
                throw e;
            }
        } else {
            // 已有粘性缓存：直接复用，省掉一次重定向跳转
            try {
                return requestExecutor.execute(currentEurekaClient);
            } catch (Exception e) {
                // 缓存的处理节点出错（可能已下线）：用 compareAndSet 失效该缓存，
                // 仅当当前缓存仍是出错的这个时才清空，避免误清掉别的线程已替换上的新缓存
                logger.info("Request execution error. endpoint={} exception={} stacktrace={}", serviceEndpoint,
                        e.getMessage(), ExceptionUtils.getStackTrace(e));
                delegateRef.compareAndSet(currentEurekaClient, null);
                currentEurekaClient.shutdown();
                throw e;
            }
        }
    }

    public static TransportClientFactory createFactory(final TransportClientFactory delegateFactory) {
        final DnsServiceImpl dnsService = new DnsServiceImpl();
        return new TransportClientFactory() {
            @Override
            public EurekaHttpClient newClient(EurekaEndpoint endpoint) {
                return new RedirectingEurekaHttpClient(endpoint.getServiceUrl(), delegateFactory, dnsService);
            }

            @Override
            public void shutdown() {
                delegateFactory.shutdown();
            }
        };
    }

    /*
     * 跟随重定向的主循环：从入口节点发请求，只要回的是 302 就解析出新目标、换 client 再发，
     * 直到收到非 302 的真实响应（命中处理节点）或超出 MAX_FOLLOWED_REDIRECTS 上限。
     * currentHttpClientRef 是"输出参数"：循环结束时它指向最终命中的那个节点的 client，
     * 由上层 execute() 取出来缓存进 delegateRef，从而形成"粘性"直连。
     */
    private <R> EurekaHttpResponse<R> executeOnNewServer(RequestExecutor<R> requestExecutor,
                                                         AtomicReference<EurekaHttpClient> currentHttpClientRef) {
        URI targetUrl = null;
        // 最多跟随 10 次 302，超过则在循环外抛 TransportException
        for (int followRedirectCount = 0; followRedirectCount < MAX_FOLLOWED_REDIRECTS; followRedirectCount++) {
            EurekaHttpResponse<R> httpResponse = requestExecutor.execute(currentHttpClientRef.get());
            // 非 302 即认为命中了真正处理请求的节点，直接返回，currentHttpClientRef 已指向它
            if (httpResponse.getStatusCode() != 302) {
                if (followRedirectCount == 0) {
                    // 第 0 次就直接成功——入口节点本身就是处理节点，无需跳转
                    logger.debug("Pinning to endpoint {}", targetUrl);
                } else {
                    logger.info("Pinning to endpoint {}, after {} redirect(s)", targetUrl, followRedirectCount);
                }
                return httpResponse;
            }

            // 收到 302：从 Location 头解析出下一跳的 base 地址（含 DNS 解析为 IP）
            targetUrl = getRedirectBaseUri(httpResponse.getLocation());
            if (targetUrl == null) {
                throw new TransportException("Invalid redirect URL " + httpResponse.getLocation());
            }

            // 关掉当前这一跳的 client，再换成指向新目标节点的 client，进入下一轮循环继续探测
            currentHttpClientRef.getAndSet(null).shutdown();
            currentHttpClientRef.set(factory.newClient(new DefaultEndpoint(targetUrl.toString())));
        }
        // 连跳 10 次仍未命中：判定链路异常，抛错让上层处理（见上面 catch 中的清理逻辑）
        String message = "Follow redirect limit crossed for URI " + serviceEndpoint.getServiceUrl();
        logger.warn(message);
        throw new TransportException(message);
    }

    /*
     * 把 302 的 Location 头规整成"可直连的下一跳服务地址"：
     *   - 校验路径符合 .../v2/apps... 的 Eureka 规范，否则视为非法重定向；
     *   - 通过 DnsService 把主机名解析成具体 IP，避免后续每次请求都重复走 DNS；
     *   - 只保留到 /v2/ 为止的前缀（group(1)）作为 base、清掉 query，得到下一个节点的服务根地址。
     */
    private URI getRedirectBaseUri(URI locationURI) {
        if (locationURI == null) {
            throw new TransportException("Missing Location header in the redirect reply");
        }
        Matcher pathMatcher = REDIRECT_PATH_REGEX.matcher(locationURI.getPath());
        if (pathMatcher.matches()) {
            return UriBuilder.fromUri(locationURI)
                    .host(dnsService.resolveIp(locationURI.getHost()))
                    .replacePath(pathMatcher.group(1))
                    .replaceQuery(null)
                    .build();
        }
        logger.warn("Invalid redirect URL {}", locationURI);
        return null;
    }
}
