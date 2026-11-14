package dev.flowforge.orchestrator;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OrchestratorApplication {
  public static void main(String[] args) {
    SpringApplication.run(OrchestratorApplication.class, args);
  }
}
