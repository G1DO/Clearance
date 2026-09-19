package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clearance.controller.jobs.Canonicalization;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * PostgreSQL-backed HTTP API verification for issue #4.
 *
 * <p>Runs against a real PostgreSQL instance (no in-memory substitution) and exercises the HTTP
 * API end to end.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class JobsApiIntegrationTest {

  private static final String KEY_ALPHA = "test-key-alpha";
  private static final String KEY_BETA = "test-key-beta";
  private static final String PROJECT_ALPHA = "project-alpha";
  private static final String PROJECT_BETA = "project-beta";

  @LocalServerPort int port;

  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper mapper;

  private final org.springframework.web.client.RestTemplate rest = nonThrowingRestTemplate();

  private static org.springframework.web.client.RestTemplate nonThrowingRestTemplate() {
    org.springframework.web.client.RestTemplate template =
        new org.springframework.web.client.RestTemplate();
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

  private String base() {
    return "http://localhost:" + port;
  }

  private HttpHeaders headers(String apiKey, String operationId) {
    HttpHeaders h = new HttpHeaders();
    h.setContentType(MediaType.APPLICATION_JSON);
    h.set("Authorization", "Bearer " + apiKey);
    if (operationId != null) {
      h.set("Idempotency-Key", operationId);
    }
    return h;
  }

  private ResponseEntity<String> postRaw(String apiKey, String operationId, String rawJson) {
    return rest.exchange(
        base() + "/api/v1/jobs", HttpMethod.POST, new HttpEntity<>(rawJson, headers(apiKey, operationId)),
        String.class);
  }

  private JsonNode json(String raw) {
    try {
      return mapper.readTree(raw);
    } catch (Exception e) {
      throw new IllegalStateException("invalid JSON in test: " + raw, e);
    }
  }

  private int countJobs(String projectId, String operationId) {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM jobs WHERE project_id = ? AND operation_id = ?",
            Integer.class,
            projectId,
            operationId);
    return n == null ? 0 : n;
  }

  private String storedHash(String projectId, String operationId) {
    return jdbc.queryForObject(
        "SELECT payload_hash FROM jobs WHERE project_id = ? AND operation_id = ?",
        String.class,
        projectId,
        operationId);
  }

  private String op() {
    return "it-" + UUID.randomUUID();
  }

  @Test
  void postCreatesAndGetReturnsDurableJob() {
    String operationId = op();
    ResponseEntity<String> post =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}");
    assertEquals(HttpStatus.CREATED, post.getStatusCode());
    JsonNode created = json(post.getBody());
    String jobId = created.get("jobId").asString();
    assertEquals(PROJECT_ALPHA, created.get("projectId").asString());
    assertEquals(operationId, created.get("operationId").asString());
    assertNotEquals(operationId, jobId, "operation_id and job_id must be distinct identities");
    String expectedHash =
        Canonicalization.payloadHash(List.of("echo", "hi"), "default");
    assertEquals(expectedHash, created.get("payloadHash").asString());

    HttpHeaders getHeaders = new HttpHeaders();
    getHeaders.set("Authorization", "Bearer " + KEY_ALPHA);
    ResponseEntity<String> get =
        rest.exchange(
            base() + "/api/v1/jobs/" + jobId, HttpMethod.GET, new HttpEntity<>(getHeaders), String.class);
    assertEquals(HttpStatus.OK, get.getStatusCode());
    JsonNode fetched = json(get.getBody());
    assertEquals(jobId, fetched.get("jobId").asString());
    assertEquals(expectedHash, fetched.get("payloadHash").asString());
    assertEquals(1, countJobs(PROJECT_ALPHA, operationId));
  }

  @Test
  void identicalRetryReturnsSameJob() {
    String operationId = op();
    String body = "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}";
    ResponseEntity<String> first = postRaw(KEY_ALPHA, operationId, body);
    assertEquals(HttpStatus.CREATED, first.getStatusCode());
    String firstJob = json(first.getBody()).get("jobId").asString();

    ResponseEntity<String> retry = postRaw(KEY_ALPHA, operationId, body);
    assertEquals(HttpStatus.OK, retry.getStatusCode());
    assertEquals(firstJob, json(retry.getBody()).get("jobId").asString());
    assertEquals(1, countJobs(PROJECT_ALPHA, operationId));
  }

  @Test
  void canonicalizationIgnoresKeyOrderAndWhitespaceButNotValues() {
    String operationId = op();
    ResponseEntity<String> first =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}");
    assertEquals(HttpStatus.CREATED, first.getStatusCode());
    String jobId = json(first.getBody()).get("jobId").asString();

    // Same semantic request, different key order and insignificant whitespace.
    ResponseEntity<String> replay =
        postRaw(
            KEY_ALPHA,
            operationId,
            "{ \"runnerClass\" : \"default\" , \"argv\" : [ \"echo\" , \"hi\" ] }");
    assertEquals(HttpStatus.OK, replay.getStatusCode());
    assertEquals(jobId, json(replay.getBody()).get("jobId").asString());

    // Changed argv element conflicts.
    ResponseEntity<String> changedArgv =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"other\"],\"runnerClass\":\"default\"}");
    assertEquals(HttpStatus.CONFLICT, changedArgv.getStatusCode());

    // Changed runnerClass conflicts.
    ResponseEntity<String> changedClass =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"large\"}");
    assertEquals(HttpStatus.CONFLICT, changedClass.getStatusCode());

    assertEquals(1, countJobs(PROJECT_ALPHA, operationId));
  }

  @Test
  void conflictLeavesOriginalUnchanged() {
    String operationId = op();
    ResponseEntity<String> first =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}");
    String jobId = json(first.getBody()).get("jobId").asString();
    String originalHash = json(first.getBody()).get("payloadHash").asString();

    ResponseEntity<String> conflict =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"other\"],\"runnerClass\":\"default\"}");
    assertEquals(HttpStatus.CONFLICT, conflict.getStatusCode());
    JsonNode conflictBody = json(conflict.getBody());
    assertEquals(jobId, conflictBody.get("existingJobId").asString());
    assertEquals(originalHash, conflictBody.get("existingPayloadHash").asString());

    assertEquals(1, countJobs(PROJECT_ALPHA, operationId));
    assertEquals(originalHash, storedHash(PROJECT_ALPHA, operationId));

    HttpHeaders getHeaders = new HttpHeaders();
    getHeaders.set("Authorization", "Bearer " + KEY_ALPHA);
    ResponseEntity<String> get =
        rest.exchange(
            base() + "/api/v1/jobs/" + jobId, HttpMethod.GET, new HttpEntity<>(getHeaders), String.class);
    assertEquals(HttpStatus.OK, get.getStatusCode());
    assertEquals(jobId, json(get.getBody()).get("jobId").asString());
  }

  @Test
  void concurrentIdenticalSubmissionsConvergeOnOneJob() throws Exception {
    String operationId = op();
    String body = "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}";
    int threads = 16;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<ResponseEntity<String>>> futures = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      Callable<ResponseEntity<String>> task =
          () -> {
            ready.countDown();
            if (!start.await(10, TimeUnit.SECONDS)) {
              throw new IllegalStateException("start gate timeout");
            }
            return postRaw(KEY_ALPHA, operationId, body);
          };
      futures.add(pool.submit(task));
    }
    assertTrue(ready.await(10, TimeUnit.SECONDS));
    start.countDown();
    Set<String> jobIds = new HashSet<>();
    for (Future<ResponseEntity<String>> f : futures) {
      ResponseEntity<String> r = f.get(30, TimeUnit.SECONDS);
      assertTrue(
          r.getStatusCode() == HttpStatus.CREATED || r.getStatusCode() == HttpStatus.OK,
          "expected 200/201 but got " + r.getStatusCode());
      jobIds.add(json(r.getBody()).get("jobId").asString());
    }
    pool.shutdownNow();
    assertEquals(1, jobIds.size(), "all identical submissions must identify the same job_id");
    assertEquals(1, countJobs(PROJECT_ALPHA, operationId));
  }

  @Test
  void concurrentConflictingPayloadLeavesOriginalUnchanged() throws Exception {
    String operationId = op();
    ResponseEntity<String> first =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}");
    String jobId = json(first.getBody()).get("jobId").asString();
    String originalHash = json(first.getBody()).get("payloadHash").asString();

    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<ResponseEntity<String>>> futures = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      Callable<ResponseEntity<String>> task =
          () -> {
            ready.countDown();
            if (!start.await(10, TimeUnit.SECONDS)) {
              throw new IllegalStateException("start gate timeout");
            }
            return postRaw(
                KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"conflict\"],\"runnerClass\":\"default\"}");
          };
      futures.add(pool.submit(task));
    }
    assertTrue(ready.await(10, TimeUnit.SECONDS));
    start.countDown();
    for (Future<ResponseEntity<String>> f : futures) {
      ResponseEntity<String> r = f.get(30, TimeUnit.SECONDS);
      assertEquals(HttpStatus.CONFLICT, r.getStatusCode());
    }
    pool.shutdownNow();
    assertEquals(1, countJobs(PROJECT_ALPHA, operationId));
    assertEquals(originalHash, storedHash(PROJECT_ALPHA, operationId));
    assertEquals(
        jobId,
        jdbc.queryForObject(
            "SELECT job_id::text FROM jobs WHERE project_id = ? AND operation_id = ?",
            String.class,
            PROJECT_ALPHA,
            operationId));
  }

  @Test
  void lostResponseRetryRecoversIdenticalJob() {
    String operationId = op();
    // Original submission commits; the client discards the response (simulated loss).
    ResponseEntity<String> original =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}");
    assertEquals(HttpStatus.CREATED, original.getStatusCode());
    String committedJobId = json(original.getBody()).get("jobId").asString();
    // Prove the row committed durably before the retry (direct DB read, not app memory).
    String committed =
        jdbc.queryForObject(
            "SELECT job_id::text FROM jobs WHERE project_id = ? AND operation_id = ?",
            String.class,
            PROJECT_ALPHA,
            operationId);
    assertEquals(committedJobId, committed);

    // Retry with the same operation identity recovers the original job.
    ResponseEntity<String> retry =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}");
    assertEquals(HttpStatus.OK, retry.getStatusCode());
    assertEquals(committedJobId, json(retry.getBody()).get("jobId").asString());
    assertEquals(1, countJobs(PROJECT_ALPHA, operationId));
  }

  @Test
  void crossProjectIsolationAndAuth() {
    String operationId = op();
    String body = "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}";

    ResponseEntity<String> alpha = postRaw(KEY_ALPHA, operationId, body);
    ResponseEntity<String> beta = postRaw(KEY_BETA, operationId, body);
    assertEquals(HttpStatus.CREATED, alpha.getStatusCode());
    assertEquals(HttpStatus.CREATED, beta.getStatusCode());
    String alphaJob = json(alpha.getBody()).get("jobId").asString();
    String betaJob = json(beta.getBody()).get("jobId").asString();
    assertNotEquals(alphaJob, betaJob, "same key in different projects must be independent");

    // Cross-project query is rejected without revealing existence.
    HttpHeaders betaHeaders = new HttpHeaders();
    betaHeaders.set("Authorization", "Bearer " + KEY_BETA);
    ResponseEntity<String> crossGet =
        rest.exchange(
            base() + "/api/v1/jobs/" + alphaJob,
            HttpMethod.GET,
            new HttpEntity<>(betaHeaders),
            String.class);
    assertEquals(HttpStatus.NOT_FOUND, crossGet.getStatusCode());

    // Missing and unknown credentials are rejected.
    ResponseEntity<String> noAuthPost =
        rest.exchange(
            base() + "/api/v1/jobs",
            HttpMethod.POST,
            new HttpEntity<>(body, jsonHeaders(null)),
            String.class);
    assertEquals(HttpStatus.UNAUTHORIZED, noAuthPost.getStatusCode());

    HttpHeaders none = new HttpHeaders();
    ResponseEntity<String> noAuthGet =
        rest.exchange(
            base() + "/api/v1/jobs/" + alphaJob, HttpMethod.GET, new HttpEntity<>(none), String.class);
    assertEquals(HttpStatus.UNAUTHORIZED, noAuthGet.getStatusCode());

    ResponseEntity<String> badKey = postRaw("wrong-key", op(), body);
    assertEquals(HttpStatus.UNAUTHORIZED, badKey.getStatusCode());
  }

  @Test
  void validationRejectsBadRequests() {
    // Empty argv.
    assertEquals(
        HttpStatus.BAD_REQUEST,
        postRaw(KEY_ALPHA, op(), "{\"argv\":[],\"runnerClass\":\"default\"}").getStatusCode());
    // Missing runnerClass.
    assertEquals(
        HttpStatus.BAD_REQUEST,
        postRaw(KEY_ALPHA, op(), "{\"argv\":[\"echo\"]}").getStatusCode());
    // Missing Idempotency-Key header.
    ResponseEntity<String> missingKey =
        rest.exchange(
            base() + "/api/v1/jobs",
            HttpMethod.POST,
            new HttpEntity<>("{\"argv\":[\"echo\"],\"runnerClass\":\"default\"}", jsonHeaders(KEY_ALPHA)),
            String.class);
    assertEquals(HttpStatus.BAD_REQUEST, missingKey.getStatusCode());
    // Blank key.
    HttpHeaders blankKey = jsonHeaders(KEY_ALPHA);
    blankKey.set("Idempotency-Key", "   ");
    ResponseEntity<String> blank =
        rest.exchange(
            base() + "/api/v1/jobs",
            HttpMethod.POST,
            new HttpEntity<>("{\"argv\":[\"echo\"],\"runnerClass\":\"default\"}", blankKey),
            String.class);
    assertEquals(HttpStatus.BAD_REQUEST, blank.getStatusCode());
    // Malformed job id.
    HttpHeaders auth = new HttpHeaders();
    auth.set("Authorization", "Bearer " + KEY_ALPHA);
    assertEquals(
        HttpStatus.BAD_REQUEST,
        rest.exchange(
                base() + "/api/v1/jobs/not-a-uuid", HttpMethod.GET, new HttpEntity<>(auth), String.class)
            .getStatusCode());
    // Unknown job id.
    assertEquals(
        HttpStatus.NOT_FOUND,
        rest.exchange(
                base() + "/api/v1/jobs/" + UUID.randomUUID(),
                HttpMethod.GET,
                new HttpEntity<>(auth),
                String.class)
            .getStatusCode());
  }

  @Test
  void migrationAndSchemaInvariants() {
    List<String> versions =
        jdbc.queryForList("SELECT version FROM flyway_schema_history ORDER BY version", String.class);
    assertTrue(versions.contains("1"), "V1 baseline must be applied");
    assertTrue(versions.contains("2"), "V2 jobs schema must be applied");

    Integer uniqueCount =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM pg_constraint WHERE conname = 'uq_jobs_project_operation'",
            Integer.class);
    assertEquals(1, uniqueCount, "UNIQUE(project_id, operation_id) must exist");

    Integer pkCount =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM pg_constraint WHERE conname = 'jobs_pkey'", Integer.class);
    assertEquals(1, pkCount, "jobs PK must exist");

    // Payload-hash invariant is enforced at the schema level.
    String checkDef =
        jdbc.queryForObject(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'chk_jobs_payload_hash'",
            String.class);
    assertTrue(checkDef != null && checkDef.contains("payload_hash"));
  }

  private HttpHeaders jsonHeaders(String apiKey) {
    HttpHeaders h = new HttpHeaders();
    h.setContentType(MediaType.APPLICATION_JSON);
    if (apiKey != null) {
      h.set("Authorization", "Bearer " + apiKey);
    }
    return h;
  }

  @Test
  void extraJsonFieldsDoNotChangeIdentity() {
    String operationId = op();
    ResponseEntity<String> first =
        postRaw(KEY_ALPHA, operationId, "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}");
    String jobId = json(first.getBody()).get("jobId").asString();
    // Unknown fields are ignored: same canonical identity.
    ResponseEntity<String> replay =
        postRaw(
            KEY_ALPHA,
            operationId,
            "{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\",\"futureField\":\"ignored\"}");
    assertEquals(HttpStatus.OK, replay.getStatusCode());
    assertEquals(jobId, json(replay.getBody()).get("jobId").asString());
  }
}
