package dev.flowforge.api;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "workflow_definition")
class WorkflowEntity {
  @Id UUID id;
  String name;
  String description;
  int revision;
  boolean enabled;

  @JdbcTypeCode(SqlTypes.JSON)
  String steps;

  @Column(name = "created_at")
  Instant createdAt;

  @Column(name = "updated_at")
  Instant updatedAt;

  @Version
  @Column(name = "lock_version")
  long lockVersion;

  protected WorkflowEntity() {}
}
