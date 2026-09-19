package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Proves idempotency survives controller restart: PostgreSQL, not process memory, is authoritative.
 *
 * <p>Starts a first application context against the real PostgreSQL, commits a job, closes the
 * context, starts a second context against the same database state, and proves the same operation
 * still deduplicates (plus Flyway restarts without schema drift).
 */
class RestartIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final RestTemplate REST = nonThrowingRestTemplate();

  private static RestTemplate nonThrowingRestTemplate() {
    RestTemplate template = new RestTemplate();
    template.setErrorHandler(
        new org.springframework.web.client.ResponseErrorHandler() {
          @Override
          public boolean hasError(org.springframework.http.client.ClientHttpResponse response) {
            return false;
          }

          @Override
          public void handleError(java.net.URI url, org.springframework.http.HttpMethod method, org.springframework.http.client.ClientHttpResponse response) {}
        });
    return template;
  }

  @Test
  void committedOperationStillDeduplicatesAfterRestart() {
    String operationId = "restart-" + UUID.randomUUID();
    String body = "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}";

    ConfigurableApplicationContext ctx1 = startApp();
    int port1 = port(ctx1);
    String jobId;
    try {
      ResponseEntity<String> first = post(port1, "test-key-alpha", operationId, body);
      assertEquals(HttpStatus.CREATED, first.getStatusCode());
      jobId = json(first.getBody()).get("jobId").asString();
    } finally {
      ctx1.close();
    }

    ConfigurableApplicationContext ctx2 = startApp();
    try {
      int port2 = port(ctx2);
      // Retry after restart recovers the original job instead of creating another.
      ResponseEntity<String> retry = post(port2, "test-key-alpha", operationId, body);
      assertEquals(HttpStatus.OK, retry.getStatusCode());
      assertEquals(jobId, json(retry.getBody()).get("jobId").asString());

      // The durable job remains queryable after restart.
      HttpHeaders getHeaders = new HttpHeaders();
      getHeaders.set("Authorization", "Bearer test-key-alpha");
      ResponseEntity<String> get =
          REST.exchange(
              "http://localhost:" + port2 + "/api/v1/jobs/" + jobId,
              HttpMethod.GET,
              new HttpEntity<>(getHeaders),
              String.class);
      assertEquals(HttpStatus.OK, get.getStatusCode());
      assertEquals(jobId, json(get.getBody()).get("jobId").asString());
    } finally {
      ctx2.close();
    }
  }

  private static ConfigurableApplicationContext startApp() {
    Map<String, Object> props = new HashMap<>();
    props.put("server.port", "0");
    return new SpringApplicationBuilder(Application.class)
        .web(WebApplicationType.SERVLET)
        .properties(props)
        .run();
  }

  private static int port(ConfigurableApplicationContext ctx) {
    Integer p = ctx.getEnvironment().getProperty("local.server.port", Integer.class);
    if (p != null && p != 0) {
      return p;
    }
    // Fallback: query the web server directly (Boot 4 package).
    org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext web =
        ctx.getBean(
            org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext
                .class);
    return web.getWebServer().getPort();
  }

  private static ResponseEntity<String> post(
      int port, String apiKey, String operationId, String rawJson) {
    HttpHeaders h = new HttpHeaders();
    h.setContentType(MediaType.APPLICATION_JSON);
    h.set("Authorization", "Bearer " + apiKey);
    h.set("Idempotency-Key", operationId);
    return REST.exchange(
        "http://localhost:" + port + "/api/v1/jobs",
        HttpMethod.POST,
        new HttpEntity<>(rawJson, h),
        String.class);
  }

  private static JsonNode json(String raw) {
    try {
      return MAPPER.readTree(raw);
    } catch (Exception e) {
      throw new IllegalStateException("invalid JSON: " + raw, e);
    }
  }
}
