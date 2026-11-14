package dev.flowforge.kotlinworker

import dev.flowforge.contracts.Contracts.StepType
import dev.flowforge.contracts.Contracts.Task
import dev.flowforge.worker.TaskFailure
import dev.flowforge.worker.TaskHandler
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication(
    scanBasePackages = ["dev.flowforge.kotlinworker", "dev.flowforge.worker"],
    proxyBeanMethods = false,
)
@EnableScheduling
class KotlinWorkerApplication {
    @Bean fun handler(): TaskHandler = JsonTransformHandler()
}

/**
 * Deterministic shallow JSON transformation with optional field removal and uppercase operations.
 */
class JsonTransformHandler : TaskHandler {
    override fun execute(task: Task): Map<String, Any> {
        if (task.type() != StepType.JSON_TRANSFORM)
            throw TaskFailure("UNSUPPORTED_TYPE", "Expected JSON_TRANSFORM", false)
        val set =
            task.configuration()["set"] as? Map<*, *>
                ?: throw TaskFailure(
                    "INVALID_CONFIGURATION",
                    "Transform requires a set object",
                    false,
                )
        val output = LinkedHashMap(task.input())
        set.forEach { (key, value) ->
            if (key !is String)
                throw TaskFailure("INVALID_CONFIGURATION", "Field names must be strings", false)
            output[key] = value
        }
        (task.configuration()["remove"] as? List<*>)?.filterIsInstance<String>()?.forEach {
            output.remove(it)
        }
        (task.configuration()["uppercase"] as? List<*>)?.filterIsInstance<String>()?.forEach { field
            ->
            (output[field] as? String)?.let { output[field] = it.uppercase(java.util.Locale.ROOT) }
        }
        return output
    }
}

fun main(args: Array<String>) {
    runApplication<KotlinWorkerApplication>(*args)
}
