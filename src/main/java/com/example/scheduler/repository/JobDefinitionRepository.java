package com.example.scheduler.repository;

import com.example.scheduler.domain.JobDefinition;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobDefinitionRepository extends JpaRepository<JobDefinition, UUID> {}
