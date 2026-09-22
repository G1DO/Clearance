package com.clearance.controller.auth;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Project-scoped API-key authentication for {@code /api/**}.
 *
 * <p>Accepts {@code Authorization: Bearer <key>} or {@code X-API-Key: <key>}, resolves the
 * authenticated {@code project_id} via {@link AuthProperties}, and exposes it as request attribute
 * {@code projectId}. Missing or unknown keys yield 401 without reaching controllers.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ApiKeyAuthFilter extends OncePerRequestFilter {

  public static final String PROJECT_ATTRIBUTE = "projectId";

  private final AuthProperties authProperties;

  public ApiKeyAuthFilter(AuthProperties authProperties) {
    this.authProperties = authProperties;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    String path = request.getRequestURI();
    return path == null || !path.startsWith("/api/");
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    String key = extractKey(request);
    String projectId = key != null ? authProperties.getApiKeys().get(key) : null;
    if (projectId == null || projectId.isBlank()
        || authProperties.getRunnerKeys().containsKey(key)) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      response.setContentType("application/json");
      byte[] body =
          "{\"error\":\"unauthorized\",\"message\":\"missing or unknown API key\"}"
              .getBytes(StandardCharsets.UTF_8);
      response.setContentLength(body.length);
      response.getOutputStream().write(body);
      return;
    }
    request.setAttribute(PROJECT_ATTRIBUTE, projectId);
    chain.doFilter(request, response);
  }

  static String extractKey(HttpServletRequest request) {
    String auth = request.getHeader("Authorization");
    if (auth != null) {
      String trimmed = auth.trim();
      if (trimmed.length() > 7 && trimmed.substring(0, 7).equalsIgnoreCase("Bearer ")) {
        String token = trimmed.substring(7).trim();
        if (!token.isEmpty()) {
          return token;
        }
      }
    }
    String apiKey = request.getHeader("X-API-Key");
    if (apiKey != null) {
      String token = apiKey.trim();
      if (!token.isEmpty()) {
        return token;
      }
    }
    return null;
  }
}
