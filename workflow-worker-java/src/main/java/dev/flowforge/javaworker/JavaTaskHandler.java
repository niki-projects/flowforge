package dev.flowforge.javaworker;

import dev.flowforge.contracts.Contracts.Task;
import dev.flowforge.worker.*;
import java.util.*;

final class JavaTaskHandler implements TaskHandler {
  private final HttpTask http;

  JavaTaskHandler(HttpTask http) {
    this.http = http;
  }

  @Override
  public Map<String, Object> execute(Task task) throws Exception {
    return switch (task.type()) {
      case DELAY -> {
        Object value = task.configuration().get("durationMs");
        if (!(value instanceof Number number)
            || number.longValue() < 0
            || number.longValue() > 60000)
          throw new TaskFailure("INVALID_CONFIGURATION", "Invalid delay duration", false);
        Thread.sleep(number.longValue());
        yield task.input();
      }
      case VALIDATE_JSON -> {
        Object value = task.configuration().get("required");
        if (!(value instanceof List<?> fields))
          throw new TaskFailure("INVALID_CONFIGURATION", "Required fields are missing", false);
        for (Object field : fields)
          if (!(field instanceof String)
              || !task.input().containsKey(field)
              || task.input().get(field) == null)
            throw new TaskFailure("VALIDATION_FAILED", "Required input field is absent", false);
        yield task.input();
      }
      case RESULT_OUTPUT -> {
        if (task.configuration().get("fields") instanceof List<?> fields) {
          Map<String, Object> selected = new LinkedHashMap<>();
          for (Object field : fields)
            if (field instanceof String name && task.input().containsKey(name))
              selected.put(name, task.input().get(name));
          yield selected;
        }
        yield task.input();
      }
      case HTTP_REQUEST -> http.execute(task);
      default ->
          throw new TaskFailure("UNSUPPORTED_TYPE", "Task is not supported by this worker", false);
    };
  }
}
