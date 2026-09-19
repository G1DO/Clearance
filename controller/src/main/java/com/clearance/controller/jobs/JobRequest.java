package com.clearance.controller.jobs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;

/**
 * v1 semantic job request body.
 *
 * <p>Intentionally narrow: a non-empty process argument vector {@code argv} (not a shell-parsed
 * command string; callers needing shell semantics request an explicit shell executable in the
 * vector) and a non-empty {@code runnerClass} compatibility input. Unknown JSON properties are
 * ignored so forward-compatible clients do not change job identity.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record JobRequest(
    @NotNull @Size(min = 1, max = 128) List<@NotNull @Size(min = 1, max = 4096) String> argv,
    @NotNull @Pattern(regexp = "[A-Za-z0-9._-]{1,128}") String runnerClass) {}
