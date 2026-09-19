package com.clearance.controller.jobs;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import tools.jackson.databind.ObjectMapper;

/**
 * Canonicalization for v1 semantic job requests.
 *
 * <p>Operates on the validated typed request ({@code argv}, {@code runnerClass}), never on raw HTTP
 * bytes. The canonical form is UTF-8 JSON {@code {"argv":[...],"runnerClass":"..."}} with keys in
 * lexicographic order, no insignificant whitespace, and RFC 8259 string escaping via Jackson. JSON
 * object-key order and serialization whitespace in the inbound payload therefore do not change job
 * identity, while {@code argv} element order and scalar values and {@code runnerClass} do.
 */
public final class Canonicalization {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Canonicalization() {}

  public static String canonicalize(List<String> argv, String runnerClass) {
    if (argv == null || runnerClass == null) {
      throw new IllegalArgumentException("argv and runnerClass must not be null");
    }
    Map<String, Object> ordered = new TreeMap<>();
    ordered.put("argv", List.copyOf(argv));
    ordered.put("runnerClass", runnerClass);
    try {
      return MAPPER.writeValueAsString(ordered);
    } catch (Exception e) {
      throw new IllegalStateException("failed to canonicalize job request", e);
    }
  }

  public static String sha256Hex(String canonicalUtf8) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(canonicalUtf8.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(64);
      for (byte b : hash) {
        hex.append(Character.forDigit((b >> 4) & 0xF, 16));
        hex.append(Character.forDigit(b & 0xF, 16));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  /** Convenience: canonical payload hash for a validated typed request. */
  public static String payloadHash(List<String> argv, String runnerClass) {
    return sha256Hex(canonicalize(argv, runnerClass));
  }
}
