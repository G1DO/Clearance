package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.clearance.controller.jobs.Canonicalization;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Canonicalization operates on the validated typed request, not raw HTTP bytes.
 */
class CanonicalizationTest {

  @Test
  void sameSemanticRequestProducesSameCanonicalAndHash() {
    String a = Canonicalization.canonicalize(List.of("echo", "hi"), "default");
    String b = Canonicalization.canonicalize(List.of("echo", "hi"), "default");
    assertEquals(a, b);
    assertEquals(
        Canonicalization.payloadHash(List.of("echo", "hi"), "default"),
        Canonicalization.payloadHash(List.of("echo", "hi"), "default"));
  }

  @Test
  void canonicalFormHasFixedKeyOrderAndNoWhitespace() {
    String canonical = Canonicalization.canonicalize(List.of("echo", "hi"), "default");
    assertEquals("{\"argv\":[\"echo\",\"hi\"],\"runnerClass\":\"default\"}", canonical);
    // 64 lowercase hex chars (SHA-256).
    String hash = Canonicalization.sha256Hex(canonical);
    assertEquals(64, hash.length());
    assertEquals(hash, hash.toLowerCase());
  }

  @Test
  void argvOrderChangesIdentity() {
    String h1 = Canonicalization.payloadHash(List.of("a", "b"), "default");
    String h2 = Canonicalization.payloadHash(List.of("b", "a"), "default");
    assertNotEquals(h1, h2);
  }

  @Test
  void argvScalarValueChangesIdentity() {
    String h1 = Canonicalization.payloadHash(List.of("echo", "hi"), "default");
    String h2 = Canonicalization.payloadHash(List.of("echo", "other"), "default");
    assertNotEquals(h1, h2);
  }

  @Test
  void runnerClassChangesIdentity() {
    String h1 = Canonicalization.payloadHash(List.of("echo", "hi"), "default");
    String h2 = Canonicalization.payloadHash(List.of("echo", "hi"), "large");
    assertNotEquals(h1, h2);
  }
}
