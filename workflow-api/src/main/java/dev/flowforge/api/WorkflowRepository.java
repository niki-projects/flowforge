package dev.flowforge.api;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

interface WorkflowRepository extends JpaRepository<WorkflowEntity, UUID> {}
