package dev.flowforge.orchestrator;

import org.springframework.context.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class KafkaConfiguration {
  @Bean
  DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka) {
    var recoverer =
        new DeadLetterPublishingRecoverer(
            kafka,
            (record, e) ->
                new org.apache.kafka.common.TopicPartition(
                    record.topic() + ".DLT", record.partition()));
    recoverer.setFailIfSendResultIsError(true);
    var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(500, 2));
    handler.setBackOffFunction(
        (record, error) -> {
          for (Throwable cause = error; cause != null; cause = cause.getCause())
            if (cause instanceof org.springframework.dao.TransientDataAccessException
                || cause instanceof org.springframework.dao.DataAccessResourceFailureException
                || cause
                    instanceof org.springframework.transaction.CannotCreateTransactionException)
              return new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS);
          return null;
        });
    return handler;
  }
}
