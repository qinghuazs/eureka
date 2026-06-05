package com.netflix.discovery.shared.transport.jersey;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import com.netflix.discovery.shared.transport.EurekaHttpResponse.EurekaHttpResponseBuilder;
import com.netflix.discovery.util.StringUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.netflix.discovery.shared.transport.EurekaHttpResponse.anEurekaHttpResponse;

/**
 * 【客户端传输装饰器链的最底层——真实 HTTP 请求层】（对应文档 06）
 *
 * 整条客户端传输链的装配顺序是：
 *   Retryable（重试/隔离区，挂一台换下一台）
 *     → Sessioned（按时长重连，刷新会话）
 *       → Redirecting（302 重定向跟随，缓存目标 Server）
 *         → AbstractJerseyEurekaHttpClient（本类，真正用 Jersey 发出 HTTP 请求）
 * 上层三个装饰器只负责"选哪台 Server、要不要重试、要不要重连"，它们最终都委托到本类来
 * 真正把请求打到 Eureka Server 的 REST 接口上。换言之，本类是所有 register/heartbeat/getApplications
 * 等操作"落地为一次真实 HTTP 调用"的地方。
 *
 * 本类把每个语义化操作映射到 Eureka Server 暴露的 REST 资源（path/方法/查询参数），并把 Jersey 的
 * {@link ClientResponse} 统一包装成传输层无关的 {@link EurekaHttpResponse}（只保留状态码、响应头、实体），
 * 这样上层装饰器就能基于"状态码"做重试/会话/重定向判断，而不必感知 Jersey 细节。
 *
 * 设计要点：
 *   - 抽象类只实现"怎么发请求 + 怎么解析响应"，把"请求头如何附加"留给子类 {@link #addExtraHeaders} 去做
 *     （JerseyApplicationClient / Jersey3 等不同实现注入各自的鉴权头、Region 头等）。
 *   - 每个方法的 finally 都负责关闭 response 释放连接，并在 debug 级别打印实际打出去的 HTTP 方法+路径+状态码。
 *
 * @author Tomasz Bak
 */
public abstract class AbstractJerseyEurekaHttpClient implements EurekaHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(AbstractJerseyEurekaHttpClient.class);
    // Server 返回 HTML 错误页（如反向代理/容器的 404、502 页面）时的 subtype 标识；
    // 用于在反序列化前甄别"这其实是一段 HTML 报错"，避免把它当 InstanceInfo/JSON 解析。
    protected static final String HTML = "html";

    protected final Client jerseyClient; // 复用的 Jersey 客户端，由对应的 EurekaHttpClientFactory 持有并管理生命周期（见 shutdown 注释）
    protected final String serviceUrl;   // 本客户端固定绑定的目标 Server 根地址，形如 http://host:port/eureka/v2/；所有请求都在它后面拼资源路径

    protected AbstractJerseyEurekaHttpClient(Client jerseyClient, String serviceUrl) {
        // 一个本类实例对应"一台确定的 Server"。上层（重定向/会话/重试层）通过工厂为不同 Server 创建不同实例，
        // 因此本类自身不做任何 Server 选择，只忠实地把请求发往构造时绑定的 serviceUrl。
        this.jerseyClient = jerseyClient;
        this.serviceUrl = serviceUrl;
        logger.debug("Created client for url: {}", serviceUrl);
    }

    /**
     * 服务注册：POST  apps/{appName}（对应文档 01 服务注册流程的"客户端发起注册"一环）。
     *
     * - HTTP 方法/路径：POST {serviceUrl}/apps/{appName}
     * - 请求体：当前实例的完整 {@link InstanceInfo}（JSON）。注册的 key 是 appName，实例身份由 body 里的 id 决定。
     * - 成功语义：Server 返回 204 No Content 表示注册被接收（部分版本/路径下为 201）。
     *   本层不解读状态码含义，只把状态码原样塞进 EurekaHttpResponse 返回给上层；是否算成功由调用方判断。
     * - 这里显式声明 Accept-Encoding: gzip 并以 JSON 收发，body 通过 post(ClientResponse.class, info) 序列化。
     */
    @Override
    public EurekaHttpResponse<Void> register(InstanceInfo info) {
        String urlPath = "apps/" + info.getAppName();
        ClientResponse response = null;
        try {
            Builder resourceBuilder = jerseyClient.resource(serviceUrl).path(urlPath).getRequestBuilder();
            addExtraHeaders(resourceBuilder); // 由子类附加鉴权/Region 等额外请求头
            response = resourceBuilder
                    .header("Accept-Encoding", "gzip")
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .accept(MediaType.APPLICATION_JSON)
                    .post(ClientResponse.class, info); // 把 InstanceInfo 作为 JSON body POST 出去
            // 注册接口无返回实体，只关心状态码与响应头，封装成传输层无关的响应对象
            return anEurekaHttpResponse(response.getStatus()).headers(headersOf(response)).build();
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Jersey HTTP POST {}{} with instance {}; statusCode={}", serviceUrl, urlPath, info.getId(),
                        response == null ? "N/A" : response.getStatus());
            }
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * 服务下线：DELETE  apps/{appName}/{id}（对应文档 04 服务下线与剔除流程的"客户端主动下线"）。
     *
     * - HTTP 方法/路径：DELETE {serviceUrl}/apps/{appName}/{id}
     * - 触发时机：应用优雅停机时调用，告知 Server 立即把该实例从注册表摘除，而不必等心跳超时被剔除。
     * - 成功语义：Server 返回 200 OK 表示删除成功。无请求体、无返回实体，只取状态码与响应头。
     */
    @Override
    public EurekaHttpResponse<Void> cancel(String appName, String id) {
        String urlPath = "apps/" + appName + '/' + id;
        ClientResponse response = null;
        try {
            Builder resourceBuilder = jerseyClient.resource(serviceUrl).path(urlPath).getRequestBuilder();
            addExtraHeaders(resourceBuilder);
            response = resourceBuilder.delete(ClientResponse.class); // DELETE 该实例资源
            return anEurekaHttpResponse(response.getStatus()).headers(headersOf(response)).build();
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Jersey HTTP DELETE {}{}; statusCode={}", serviceUrl, urlPath, response == null ? "N/A" : response.getStatus());
            }
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * 心跳续约：PUT  apps/{appName}/{id}?status=...&lastDirtyTimestamp=...（对应文档 02 心跳续约流程的核心调用）。
     *
     * - HTTP 方法/路径：PUT {serviceUrl}/apps/{appName}/{id}
     * - 查询参数：
     *     status                ——当前实例状态（UP/DOWN/...），让 Server 校验/同步状态；
     *     lastDirtyTimestamp    ——本地最近一次实例数据变更的时间戳，Server 用它判断双方数据是否一致（防止脏数据覆盖新数据）；
     *     overriddenstatus（可选）——存在状态覆盖时一并带上，便于 Server 对齐覆盖状态。
     * - 关键状态码语义（上层及 DiscoveryClient 据此决策）：
     *     200 OK        ——续约成功，租约时钟被重置，无需进一步动作；
     *     404 Not Found ——Server 上已不存在该实例（如曾被剔除/Server 重启丢失），调用方需"重新走一遍 register"补注册。
     * - 返回实体：当 Server 检测到 lastDirtyTimestamp 不匹配时，会在响应体回传它那边更新的 {@link InstanceInfo}，
     *   供客户端比对/回补；因此这里会尝试反序列化响应实体（但要先排除 HTML 错误页，见下）。
     */
    @Override
    public EurekaHttpResponse<InstanceInfo> sendHeartBeat(String appName, String id, InstanceInfo info, InstanceStatus overriddenStatus) {
        String urlPath = "apps/" + appName + '/' + id;
        ClientResponse response = null;
        try {
            WebResource webResource = jerseyClient.resource(serviceUrl)
                    .path(urlPath)
                    .queryParam("status", info.getStatus().toString())
                    .queryParam("lastDirtyTimestamp", info.getLastDirtyTimestamp().toString());
            if (overriddenStatus != null) {
                // 仅当存在覆盖状态时才追加该参数，避免无谓的查询串
                webResource = webResource.queryParam("overriddenstatus", overriddenStatus.name());
            }
            Builder requestBuilder = webResource.getRequestBuilder();
            addExtraHeaders(requestBuilder);
            response = requestBuilder.put(ClientResponse.class); // 续约是一次无 body 的 PUT
            EurekaHttpResponseBuilder<InstanceInfo> eurekaResponseBuilder = anEurekaHttpResponse(response.getStatus(), InstanceInfo.class).headers(headersOf(response));
            if (response.hasEntity() &&
                    !HTML.equals(response.getType().getSubtype())) { //don't try and deserialize random html errors from the server
                // 仅在确有 JSON 实体（而非 HTML 报错页）时才反序列化为 InstanceInfo，防止把代理/容器的 HTML 错误页当数据解析报错
                eurekaResponseBuilder.entity(response.getEntity(InstanceInfo.class));
            }
            return eurekaResponseBuilder.build();
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Jersey HTTP PUT {}{}; statusCode={}", serviceUrl, urlPath, response == null ? "N/A" : response.getStatus());
            }
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * 设置状态覆盖：PUT  apps/{appName}/{id}/status?value=...&lastDirtyTimestamp=...（对应文档 07 状态覆盖规则链）。
     *
     * - HTTP 方法/路径：PUT {serviceUrl}/apps/{appName}/{id}/status
     * - 查询参数：value=目标状态（如手动置 OUT_OF_SERVICE 摘流量）、lastDirtyTimestamp=数据版本时间戳。
     * - 作用：人为给某实例打上"覆盖状态"，让 Server 在状态覆盖规则链里优先采用此值（常用于运维手动上下线）。
     * - 成功语义：200 OK。无返回实体。
     */
    @Override
    public EurekaHttpResponse<Void> statusUpdate(String appName, String id, InstanceStatus newStatus, InstanceInfo info) {
        String urlPath = "apps/" + appName + '/' + id + "/status";
        ClientResponse response = null;
        try {
            Builder requestBuilder = jerseyClient.resource(serviceUrl)
                    .path(urlPath)
                    .queryParam("value", newStatus.name())
                    .queryParam("lastDirtyTimestamp", info.getLastDirtyTimestamp().toString())
                    .getRequestBuilder();
            addExtraHeaders(requestBuilder);
            response = requestBuilder.put(ClientResponse.class);
            return anEurekaHttpResponse(response.getStatus()).headers(headersOf(response)).build();
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Jersey HTTP PUT {}{}; statusCode={}", serviceUrl, urlPath, response == null ? "N/A" : response.getStatus());
            }
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * 撤销状态覆盖：DELETE  apps/{appName}/{id}/status?lastDirtyTimestamp=...（对应文档 07 状态覆盖规则链）。
     *
     * - HTTP 方法/路径：DELETE {serviceUrl}/apps/{appName}/{id}/status
     * - 作用：与 statusUpdate 相反，删掉之前打的覆盖状态，让实例状态回归由心跳/规则自然推导（运维"取消手动下线"）。
     * - 成功语义：200 OK。仍需带 lastDirtyTimestamp 供 Server 做版本比对。
     */
    @Override
    public EurekaHttpResponse<Void> deleteStatusOverride(String appName, String id, InstanceInfo info) {
        String urlPath = "apps/" + appName + '/' + id + "/status";
        ClientResponse response = null;
        try {
            Builder requestBuilder = jerseyClient.resource(serviceUrl)
                    .path(urlPath)
                    .queryParam("lastDirtyTimestamp", info.getLastDirtyTimestamp().toString())
                    .getRequestBuilder();
            addExtraHeaders(requestBuilder);
            response = requestBuilder.delete(ClientResponse.class);
            return anEurekaHttpResponse(response.getStatus()).headers(headersOf(response)).build();
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Jersey HTTP DELETE {}{}; statusCode={}", serviceUrl, urlPath, response == null ? "N/A" : response.getStatus());
            }
            if (response != null) {
                response.close();
            }
        }
    }

    // ============================ 以下为只读拉取类操作（对应文档 03 拉取注册表流程） ============================
    // 这一组方法都是 GET，区别只在资源路径不同；它们共用下面的 getApplicationsInternal 完成发请求+解析。

    /**
     * 全量拉取注册表：GET  apps/（首次启动 / delta 失效兜底时拉取全部应用与实例）。
     * 可选 regions 查询参数用于跨 Region 拉取远端注册表。返回 200 + Applications（JSON）。
     */
    @Override
    public EurekaHttpResponse<Applications> getApplications(String... regions) {
        return getApplicationsInternal("apps/", regions);
    }

    /**
     * 增量拉取注册表：GET  apps/delta（周期性只拉最近变更，配合本地缓存合并，省带宽，是常态拉取方式）。
     */
    @Override
    public EurekaHttpResponse<Applications> getDelta(String... regions) {
        return getApplicationsInternal("apps/delta", regions);
    }

    /**
     * 按 VIP（虚拟地址）拉取：GET  vips/{vipAddress}（只取挂在该 VIP 下的实例）。
     */
    @Override
    public EurekaHttpResponse<Applications> getVip(String vipAddress, String... regions) {
        return getApplicationsInternal("vips/" + vipAddress, regions);
    }

    /**
     * 按 Secure VIP（HTTPS 虚拟地址）拉取：GET  svips/{secureVipAddress}。
     */
    @Override
    public EurekaHttpResponse<Applications> getSecureVip(String secureVipAddress, String... regions) {
        return getApplicationsInternal("svips/" + secureVipAddress, regions);
    }

    /**
     * 上面四个拉取入口的公共实现：拼好（可选的）regions 参数后发 GET，并把响应体反序列化为 {@link Applications}。
     * 只有当状态码为 200 OK 且确有实体时才解析实体，否则实体置 null（让上层据状态码处理）。
     */
    private EurekaHttpResponse<Applications> getApplicationsInternal(String urlPath, String[] regions) {
        ClientResponse response = null;
        String regionsParamValue = null;
        try {
            WebResource webResource = jerseyClient.resource(serviceUrl).path(urlPath);
            if (regions != null && regions.length > 0) {
                // 有远端 Region 才拼 regions 查询参数（多个 Region 以逗号连接）
                regionsParamValue = StringUtil.join(regions);
                webResource = webResource.queryParam("regions", regionsParamValue);
            }
            Builder requestBuilder = webResource.getRequestBuilder();
            addExtraHeaders(requestBuilder);
            response = requestBuilder.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class); // 以 JSON 接收注册表

            Applications applications = null;
            // 仅 200 且有实体才解析；非 2xx（如 304/4xx/5xx）实体保持 null，交由上层根据状态码处理
            if (response.getStatus() == Status.OK.getStatusCode() && response.hasEntity()) {
                applications = response.getEntity(Applications.class);
            }
            return anEurekaHttpResponse(response.getStatus(), Applications.class)
                    .headers(headersOf(response))
                    .entity(applications)
                    .build();
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Jersey HTTP GET {}{}?{}; statusCode={}",
                        serviceUrl, urlPath,
                        regionsParamValue == null ? "" : "regions=" + regionsParamValue,
                        response == null ? "N/A" : response.getStatus()
                );
            }
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * 拉取单个应用：GET  apps/{appName}（只取某个应用下的全部实例，比全量拉取更精准）。
     * 同样仅在 200 OK 且有实体时反序列化为 {@link Application}。
     */
    @Override
    public EurekaHttpResponse<Application> getApplication(String appName) {
        String urlPath = "apps/" + appName;
        ClientResponse response = null;
        try {
            Builder requestBuilder = jerseyClient.resource(serviceUrl).path(urlPath).getRequestBuilder();
            addExtraHeaders(requestBuilder);
            response = requestBuilder.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

            Application application = null;
            if (response.getStatus() == Status.OK.getStatusCode() && response.hasEntity()) {
                application = response.getEntity(Application.class);
            }
            return anEurekaHttpResponse(response.getStatus(), Application.class)
                    .headers(headersOf(response))
                    .entity(application)
                    .build();
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Jersey HTTP GET {}{}; statusCode={}", serviceUrl, urlPath, response == null ? "N/A" : response.getStatus());
            }
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * 按实例 id 全局查询：GET  instances/{id}（不限应用，直接按实例 id 找）。
     */
    @Override
    public EurekaHttpResponse<InstanceInfo> getInstance(String id) {
        return getInstanceInternal("instances/" + id);
    }

    /**
     * 按应用+实例 id 查询：GET  apps/{appName}/{id}（定位到具体应用下的某个实例）。
     */
    @Override
    public EurekaHttpResponse<InstanceInfo> getInstance(String appName, String id) {
        return getInstanceInternal("apps/" + appName + '/' + id);
    }

    /**
     * 上面两个实例查询入口的公共实现：发 GET 并把响应体解析为 {@link InstanceInfo}。
     * 变量名 infoFromPeer 暗示该接口在 Server 集群间互查实例时也会用到（从对端 peer 取实例数据）。
     */
    private EurekaHttpResponse<InstanceInfo> getInstanceInternal(String urlPath) {
        ClientResponse response = null;
        try {
            Builder requestBuilder = jerseyClient.resource(serviceUrl).path(urlPath).getRequestBuilder();
            addExtraHeaders(requestBuilder);
            response = requestBuilder.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

            InstanceInfo infoFromPeer = null;
            if (response.getStatus() == Status.OK.getStatusCode() && response.hasEntity()) {
                infoFromPeer = response.getEntity(InstanceInfo.class);
            }
            return anEurekaHttpResponse(response.getStatus(), InstanceInfo.class)
                    .headers(headersOf(response))
                    .entity(infoFromPeer)
                    .build();
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Jersey HTTP GET {}{}; statusCode={}", serviceUrl, urlPath, response == null ? "N/A" : response.getStatus());
            }
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * 关闭本客户端：刻意"什么都不做"。
     * 因为底层的 jerseyClient（连接池）由对应的 EurekaHttpClientFactory 统一持有与销毁，本类只是借用，
     * 若在此销毁会误伤其他复用同一个 jerseyClient 的客户端实例。所以这里只留注释说明意图，不做实际关闭。
     */
    @Override
    public void shutdown() {
        // Do not destroy jerseyClient, as it is owned by the corresponding EurekaHttpClientFactory
    }

    // 由子类实现：在每个请求发出前附加额外请求头（如鉴权 token、Region、用户自定义头）。
    // 抽象出去是为了让本类专注"发什么请求"，把"带什么头"这种实现/环境相关的差异交给具体子类。
    protected abstract void addExtraHeaders(Builder webResource);

    // 把 Jersey 的多值响应头（MultivaluedMap）压平成普通 Map<String,String>：每个头只取第一个值。
    // 上层装饰器（如重定向层读 Location 头）只需单值即可，这样屏蔽了 Jersey 类型，保持 EurekaHttpResponse 纯净。
    private static Map<String, String> headersOf(ClientResponse response) {
        MultivaluedMap<String, String> jerseyHeaders = response.getHeaders();
        if (jerseyHeaders == null || jerseyHeaders.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> headers = new HashMap<>();
        for (Entry<String, List<String>> entry : jerseyHeaders.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                headers.put(entry.getKey(), entry.getValue().get(0));
            }
        }
        return headers;
    }
}
