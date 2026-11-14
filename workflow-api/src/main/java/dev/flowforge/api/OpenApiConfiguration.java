package dev.flowforge.api;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.*;
import org.springframework.context.annotation.*;

@Configuration
class OpenApiConfiguration {
  @Bean
  OpenAPI openApi() {
    return new OpenAPI()
        .info(
            new Info()
                .title("FlowForge Management API")
                .version("v1")
                .description(
                    "Asynchronous sequential workflows. DELETE disables definitions. Retry creates a new execution using the current definition."))
        .components(
            new Components()
                .addSecuritySchemes(
                    "apiKey",
                    new SecurityScheme()
                        .type(SecurityScheme.Type.APIKEY)
                        .in(SecurityScheme.In.HEADER)
                        .name("X-API-Key")))
        .addSecurityItem(new SecurityRequirement().addList("apiKey"));
  }
}
