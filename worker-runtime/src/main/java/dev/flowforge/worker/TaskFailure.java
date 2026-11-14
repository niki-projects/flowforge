package dev.flowforge.worker;

public final class TaskFailure extends RuntimeException {
  private final String code;
  private final boolean retryable;

  public TaskFailure(String code, String message, boolean retryable) {
    super(message);
    this.code = code;
    this.retryable = retryable;
  }

  public String code() {
    return code;
  }

  public boolean retryable() {
    return retryable;
  }
}
