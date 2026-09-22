package com.clearance.controller.auth;

import com.clearance.controller.agent.AgentProtocol;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Separate machine authentication for the internal agent routes; never grants operator access. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class RunnerKeyAuthFilter extends OncePerRequestFilter {

  public static final String RUNNER_ATTRIBUTE = "runnerId";
  public static final String FAULT_PATH = "/internal/v1/agents/test/faults";
  private static final Set<String> AGENT_PATHS =
      Set.of("/internal/v1/agents/poll", "/internal/v1/agents/report", FAULT_PATH);

  private final AuthProperties authProperties;
  private final boolean testProfile;

  public RunnerKeyAuthFilter(AuthProperties authProperties, Environment environment) {
    this.authProperties = authProperties;
    this.testProfile = environment.acceptsProfiles(Profiles.of("test"));
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    String path = request.getRequestURI();
    // Outside test the fault controller does not exist. Preserve its ordinary 404 even
    // without credentials, rather than exposing an authentication-shaped test endpoint.
    return path == null || !path.startsWith("/internal/")
        || (FAULT_PATH.equals(path) && !testProfile);
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    String key = ApiKeyAuthFilter.extractKey(request);
    UUID runnerId = key == null ? null : authProperties.getRunnerKeys().get(key);
    if (runnerId == null || authProperties.getApiKeys().containsKey(key)) {
      error(response, 401, "unauthorized", "missing or unknown separate runner key");
      return;
    }
    if (!AGENT_PATHS.contains(request.getRequestURI())) {
      error(response, 403, "fenced_rejected", "runner identity cannot call this internal action");
      return;
    }
    request.setAttribute(RUNNER_ATTRIBUTE, runnerId);
    chain.doFilter(request, response);
  }

  private static void error(
      HttpServletResponse response, int status, String code, String message) throws IOException {
    byte[] body = AgentProtocol.encodeErrorBody(new AgentProtocol.ErrorBody(code, message))
        .getBytes(StandardCharsets.UTF_8);
    response.setStatus(status);
    response.setContentType("application/json");
    response.setContentLength(body.length);
    response.getOutputStream().write(body);
  }
}
