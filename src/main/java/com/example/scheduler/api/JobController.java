package com.example.scheduler.api;

import com.example.scheduler.api.dto.*;
import com.example.scheduler.domain.ExecutionStatus;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.roles.api", havingValue = "true", matchIfMissing = true)
public class JobController {

  private final JobService service;

  @PostMapping
  public ResponseEntity<JobResponse> create(@Valid @RequestBody JobRequest req) {
    JobResponse r = service.create(req);
    return ResponseEntity.created(URI.create("/api/v1/jobs/" + r.id())).body(r);
  }

  @GetMapping("/{id}")
  public JobResponse get(@PathVariable UUID id) { return service.get(id); }

  @PutMapping("/{id}")
  public JobResponse update(@PathVariable UUID id, @Valid @RequestBody UpdateJobRequest req) {
    return service.update(id, req);
  }

  @PutMapping("/{id}/pause")
  public JobResponse pause(@PathVariable UUID id) { return service.pause(id); }

  @PutMapping("/{id}/resume")
  public JobResponse resume(@PathVariable UUID id) { return service.resume(id); }

  @DeleteMapping("/{id}")
  public ResponseEntity<Void> cancel(@PathVariable UUID id) {
    service.cancel(id);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/{id}/trigger")
  public ResponseEntity<ManualTriggerResponse> trigger(@PathVariable UUID id) {
    return ResponseEntity.accepted().body(service.triggerNow(id));
  }

  @GetMapping("/{id}/history")
  public Page<ExecutionView> history(@PathVariable UUID id,
      @RequestParam(required = false) ExecutionStatus status,
      @PageableDefault(size = 50, sort = "scheduledFireTime", direction = Sort.Direction.DESC) Pageable page) {
    return service.history(id, status, page);
  }
}
