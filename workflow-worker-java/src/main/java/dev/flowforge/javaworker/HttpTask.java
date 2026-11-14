package dev.flowforge.javaworker;

import dev.flowforge.contracts.Contracts.Task;
import dev.flowforge.worker.TaskFailure;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** GET-only HTTP integration: exact origin allowlist, no redirects or user headers. */
@Component
final class HttpTask {
  private final boolean enabled;
  private final Set<String> origins;
  private final HttpClient client =
      HttpClient.newBuilder()
          .followRedirects(HttpClient.Redirect.NEVER)
          .connectTimeout(Duration.ofSeconds(3))
          .build();

  HttpTask(
      @Value("${flowforge.http.enabled:false}") boolean enabled,
      @Value("${flowforge.http.allowed-origins:}") String origins) {
    this.enabled = enabled;
    this.origins = new HashSet<>(Arrays.asList(origins.split(",")));
  }

  Map<String, Object> execute(Task task) throws Exception {
    if (!enabled) throw new TaskFailure("HTTP_DISABLED", "HTTP tasks are disabled", false);
    URI uri;
    try {
      uri = URI.create(Objects.toString(task.configuration().get("url"), ""));
    } catch (IllegalArgumentException e) {
      throw blocked();
    }
    validate(uri);
    var request =
        HttpRequest.newBuilder(uri)
            .timeout(Duration.ofMillis(Math.min(10000, task.timeoutMs())))
            .GET()
            .header("Accept", "application/json")
            .build();
    try {
      var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
      try (var body = response.body()) {
        classifyStatus(response.statusCode());
        byte[] bytes = body.readNBytes(65537);
        if (bytes.length > 65536)
          throw new TaskFailure(
              "HTTP_RESPONSE_TOO_LARGE", "Upstream response exceeded limit", false);
        var output = new LinkedHashMap<>(task.input());
        output.put("httpStatus", response.statusCode());
        output.put("httpBody", new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
        return output;
      }
    } catch (java.io.IOException e) {
      throw new TaskFailure("HTTP_IO_ERROR", "Upstream connection failed", true);
    }
  }

  void validate(URI uri) throws UnknownHostException {
    if (!"https".equalsIgnoreCase(uri.getScheme())
        || uri.getHost() == null
        || uri.getUserInfo() != null
        || uri.getFragment() != null) throw blocked();
    String origin =
        "https://"
            + uri.getHost().toLowerCase(Locale.ROOT)
            + (uri.getPort() == -1 || uri.getPort() == 443 ? "" : ":" + uri.getPort());
    if (!origins.contains(origin)) throw blocked();
    for (InetAddress address : InetAddress.getAllByName(uri.getHost())) {
      byte[] bytes = address.getAddress();
      int a = bytes[0] & 255, b = bytes.length > 1 ? bytes[1] & 255 : 0;
      if (address.isAnyLocalAddress()
          || address.isLoopbackAddress()
          || address.isLinkLocalAddress()
          || address.isSiteLocalAddress()
          || address.isMulticastAddress()
          || (bytes.length == 4
              && (a == 0
                  || a >= 224
                  || (a == 100 && b >= 64 && b <= 127)
                  || (a == 169 && b == 254)
                  || (a == 198 && (b == 18 || b == 19))))
          || (bytes.length == 16 && ((a & 0xfe) == 0xfc || a == 0))) throw blocked();
    }
  }

  static void classifyStatus(int status) {
    if (status >= 300 && status < 400)
      throw new TaskFailure("HTTP_REDIRECT_BLOCKED", "Redirects are disabled", false);
    if (status == 429 || status >= 500)
      throw new TaskFailure("HTTP_TRANSIENT", "Upstream temporarily unavailable", true);
    if (status >= 400) throw new TaskFailure("HTTP_REJECTED", "Upstream rejected request", false);
  }

  private static TaskFailure blocked() {
    return new TaskFailure(
        "HTTP_DESTINATION_BLOCKED", "Destination is not allowed by HTTP policy", false);
  }
}
