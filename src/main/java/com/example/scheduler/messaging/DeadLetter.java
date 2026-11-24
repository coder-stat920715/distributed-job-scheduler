package com.example.scheduler.messaging;

import java.util.UUID;

public record DeadLetter(UUID executionId, UUID jobId, int attempt, String error) {}
