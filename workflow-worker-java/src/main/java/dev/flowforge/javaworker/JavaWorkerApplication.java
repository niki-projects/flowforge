package dev.flowforge.javaworker;

import dev.flowforge.worker.TaskHandler;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"dev.flowforge.javaworker", "dev.flowforge.worker"})
@EnableScheduling
public class JavaWorkerApplication {
  public static void main(String[] args) {
    SpringApplication.run(JavaWorkerApplication.class, args);
  }

  @Bean
  TaskHandler handler(HttpTask http) {
    return new JavaTaskHandler(http);
  }
}
