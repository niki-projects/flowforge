package dev.flowforge.kotlinworker

import dev.flowforge.contracts.Contracts.*
import java.time.Instant
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class JsonTransformHandlerTest {
    @Test
    fun `transforms data without mutating input`() {
        val input = mapOf<String, Any>("name" to "Ada", "temporary" to true)
        val task =
            Task(
                UUID.randomUUID(),
                1,
                Instant.now(),
                "test",
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                StepType.JSON_TRANSFORM,
                mapOf(
                    "set" to mapOf("processed" to true),
                    "remove" to listOf("temporary"),
                    "uppercase" to listOf("name"),
                ),
                input,
                1000,
            )
        val result = JsonTransformHandler().execute(task)
        assertThat(result)
            .containsEntry("name", "ADA")
            .containsEntry("processed", true)
            .doesNotContainKey("temporary")
        assertThat(input).containsEntry("name", "Ada").containsKey("temporary")
    }
}
