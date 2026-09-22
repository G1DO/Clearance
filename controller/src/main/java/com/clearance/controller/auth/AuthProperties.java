package com.clearance.controller.auth;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Static API-key to project mapping for v1.
 *
 * <p>v1 obtains {@code project_id} exclusively from this authenticated mapping, never from the
 * request body. The map is intentionally bounded static configuration, not a growing
 * request-scoped deduplication structure; idempotency correctness lives in PostgreSQL.
 */
@Component
@ConfigurationProperties(prefix = "clearance.auth")
public class AuthProperties {

  /** API key token to project_id. Configured in application.yml (overridable via env). */
  private Map<String, String> apiKeys = new HashMap<>();

  /** Separate machine credentials: runner token to seeded runner identity. */
  private Map<String, UUID> runnerKeys = new HashMap<>();

  public Map<String, UUID> getRunnerKeys() {
    return runnerKeys;
  }

  public void setRunnerKeys(Map<String, UUID> runnerKeys) {
    this.runnerKeys = runnerKeys != null ? runnerKeys : new HashMap<>();
  }

  public Map<String, String> getApiKeys() {
    return apiKeys;
  }

  public void setApiKeys(Map<String, String> apiKeys) {
    this.apiKeys = apiKeys != null ? apiKeys : new HashMap<>();
  }
}
