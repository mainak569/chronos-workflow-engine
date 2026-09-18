package com.chronos.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Gateway behaviour with the backend unreachable (nothing listens on port 1).
 */
@SpringBootTest(properties = {
        "gateway.workflow-service-url=http://127.0.0.1:1",
        "gateway.connect-timeout=1s",
        "jwt.secret=test-secret-key-for-jwt-signing-must-be-at-least-256-bits-long"
})
@AutoConfigureMockMvc
class ApiGatewayApplicationTests {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void rejectsApiCallsWithoutTokenBeforeReachingBackend() throws Exception {
        mockMvc.perform(get("/api/v1/workflows"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error").value("UNAUTHORIZED"))
                .andExpect(header().exists("X-Correlation-ID"));
    }

    @Test
    void rejectsInvalidToken() throws Exception {
        mockMvc.perform(get("/api/v1/workflows").header("Authorization", "Bearer not-a-jwt"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.message").value("Invalid or expired token"));
    }

    @Test
    void echoesIncomingCorrelationId() throws Exception {
        mockMvc.perform(get("/api/v1/workflows").header("X-Correlation-ID", "abc-123"))
                .andExpect(header().string("X-Correlation-ID", "abc-123"))
                .andExpect(jsonPath("$.correlationId").value("abc-123"));
    }

    @Test
    void publicAuthRoutesAreForwardedAndBackendFailureIsReportedAsBadGateway() throws Exception {
        mockMvc.perform(post("/api/v1/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"email\":\"a@example.com\",\"password\":\"secret123\"}"))
                .andExpect(status().isBadGateway())
                .andExpect(jsonPath("$.error").value("BAD_GATEWAY"));
    }

    @Test
    void readinessProbeIsServedOnTheApplicationPort() throws Exception {
        mockMvc.perform(get("/readyz"))
                .andExpect(status().isOk());
    }
}
