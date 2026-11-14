package dev.flowforge.api;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
class ApiGuard extends OncePerRequestFilter {
  private static final int LIMIT = 262144;
  private final byte[] key;
  private final StringRedisTemplate redis;
  private static final DefaultRedisScript<Long> RATE =
      new DefaultRedisScript<>(
          "local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],60) end; return n",
          Long.class);

  ApiGuard(@Value("${flowforge.api-key}") String key, StringRedisTemplate redis) {
    if (key.length() < 16)
      throw new IllegalArgumentException("FLOWFORGE_API_KEY must contain at least 16 characters");
    this.key = key.getBytes(StandardCharsets.UTF_8);
    this.redis = redis;
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    String correlation = request.getHeader("X-Correlation-ID");
    if (correlation == null || !correlation.matches("[A-Za-z0-9._-]{1,100}"))
      correlation = UUID.randomUUID().toString();
    MDC.put("correlationId", correlation);
    response.setHeader("X-Correlation-ID", correlation);
    response.setHeader("X-Content-Type-Options", "nosniff");
    try {
      if (request.getRequestURI().startsWith("/actuator/health")) {
        chain.doFilter(request, response);
        return;
      }
      // Swagger assets are public; its requests still require the explicit API key.
      boolean docs =
          request.getRequestURI().startsWith("/swagger-ui")
              || request.getRequestURI().startsWith("/v3/api-docs");
      String supplied = Objects.toString(request.getHeader("X-API-Key"), "");
      if (!docs && !MessageDigest.isEqual(key, supplied.getBytes(StandardCharsets.UTF_8))) {
        reject(response, 401, "UNAUTHORIZED");
        return;
      }
      if (!docs && request.getRequestURI().startsWith("/api/")) {
        try {
          Long count = redis.execute(RATE, List.of("flowforge:rate:management"));
          if (count != null && count > 120) {
            response.setHeader("Retry-After", "60");
            reject(response, 429, "RATE_LIMITED");
            return;
          }
        } catch (org.springframework.data.redis.RedisConnectionFailureException
            | org.springframework.dao.QueryTimeoutException e) {
          // Redis limits are advisory. PostgreSQL remains available during Redis outages.
        }
      }
      if (request.getContentLengthLong() > LIMIT) {
        reject(response, 413, "PAYLOAD_TOO_LARGE");
        return;
      }
      if (Set.of("POST", "PUT", "PATCH").contains(request.getMethod())) {
        byte[] body = request.getInputStream().readNBytes(LIMIT + 1);
        if (body.length > LIMIT) {
          reject(response, 413, "PAYLOAD_TOO_LARGE");
          return;
        }
        chain.doFilter(
            new HttpServletRequestWrapper(request) {
              @Override
              public ServletInputStream getInputStream() {
                var stream = new ByteArrayInputStream(body);
                return new ServletInputStream() {
                  public int read() {
                    return stream.read();
                  }

                  public boolean isFinished() {
                    return stream.available() == 0;
                  }

                  public boolean isReady() {
                    return true;
                  }

                  public void setReadListener(ReadListener listener) {
                    throw new UnsupportedOperationException();
                  }
                };
              }

              @Override
              public BufferedReader getReader() {
                return new BufferedReader(
                    new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
              }
            },
            response);
      } else chain.doFilter(request, response);
    } finally {
      MDC.clear();
    }
  }

  private static void reject(HttpServletResponse response, int status, String code)
      throws IOException {
    response.setStatus(status);
    response.setContentType("application/problem+json");
    response
        .getWriter()
        .write(
            "{\"type\":\"urn:flowforge:problem:"
                + code.toLowerCase(Locale.ROOT)
                + "\",\"status\":"
                + status
                + ",\"title\":\""
                + code
                + "\",\"code\":\""
                + code
                + "\"}");
  }
}
