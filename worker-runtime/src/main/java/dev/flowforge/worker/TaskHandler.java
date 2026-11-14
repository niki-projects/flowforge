package dev.flowforge.worker;

import dev.flowforge.contracts.Contracts.Task;
import java.util.Map;

@FunctionalInterface
public interface TaskHandler {
  Map<String, Object> execute(Task task) throws Exception;
}
