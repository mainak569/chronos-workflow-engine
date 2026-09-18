package com.chronos.gateway.proxy;

import com.chronos.gateway.config.GatewayProperties;
import com.chronos.gateway.error.ErrorResponses;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Forwards /api/** requests to workflow-service (which owns the public REST API)
 * and relays the response unchanged.
 */
@RestController
public class ProxyController {

    private static final Logger log = LoggerFactory.getLogger(ProxyController.class);

    /**
     * Hop-by-hop headers and headers the HTTP client manages itself are not forwarded.
     */
    private static final Set<String> EXCLUDED_HEADERS = Set.of(
            "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
            "transfer-encoding", "upgrade", "host", "content-length", "expect", "http2-settings");

    private final GatewayProperties properties;
    private final HttpClient httpClient;

    public ProxyController(GatewayProperties properties) {
        this.properties = properties;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(properties.connectTimeout())
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    @RequestMapping("/api/**")
    public ResponseEntity<?> proxy(HttpServletRequest request) throws IOException {
        String query = request.getQueryString();
        URI target = URI.create(properties.workflowServiceUrl() + request.getRequestURI()
                + (query != null ? "?" + query : ""));

        byte[] body = StreamUtils.copyToByteArray(request.getInputStream());
        HttpRequest.Builder builder = HttpRequest.newBuilder(target)
                .timeout(properties.readTimeout())
                .method(request.getMethod(), body.length > 0
                        ? HttpRequest.BodyPublishers.ofByteArray(body)
                        : HttpRequest.BodyPublishers.noBody());

        for (String name : Collections.list(request.getHeaderNames())) {
            if (!EXCLUDED_HEADERS.contains(name.toLowerCase())) {
                for (String value : Collections.list(request.getHeaders(name))) {
                    builder.header(name, value);
                }
            }
        }
        Object correlationId = request.getAttribute(ErrorResponses.CorrelationIds.ATTRIBUTE);
        if (correlationId != null) {
            builder.setHeader(ErrorResponses.CorrelationIds.HEADER, correlationId.toString());
        }
        builder.header("X-Forwarded-For", request.getRemoteAddr());

        try {
            HttpResponse<byte[]> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());

            HttpHeaders headers = new HttpHeaders();
            response.headers().map().forEach((name, values) -> {
                if (!EXCLUDED_HEADERS.contains(name.toLowerCase()) && !name.startsWith(":")) {
                    headers.put(name, List.copyOf(values));
                }
            });
            return ResponseEntity.status(response.statusCode()).headers(headers).body(response.body());

        } catch (HttpConnectTimeoutException | ConnectException e) {
            log.error("Backend unavailable: {} {}", request.getMethod(), target, e);
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(ErrorResponses.body(HttpStatus.BAD_GATEWAY,
                    "BAD_GATEWAY", "Workflow service is unavailable", request));
        } catch (HttpTimeoutException e) {
            log.error("Backend timed out: {} {}", request.getMethod(), target);
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).body(ErrorResponses.body(HttpStatus.GATEWAY_TIMEOUT,
                    "GATEWAY_TIMEOUT", "Workflow service did not respond in time", request));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ErrorResponses.body(
                    HttpStatus.SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE", "Gateway is shutting down", request));
        } catch (IOException e) {
            log.error("Error proxying request: {} {}", request.getMethod(), target, e);
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(ErrorResponses.body(HttpStatus.BAD_GATEWAY,
                    "BAD_GATEWAY", "Error communicating with workflow service", request));
        }
    }
}
