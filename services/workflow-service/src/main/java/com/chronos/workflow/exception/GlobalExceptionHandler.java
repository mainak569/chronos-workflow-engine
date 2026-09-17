package com.chronos.workflow.exception;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Global exception handler for REST controllers.
 * Provides consistent error responses across the API.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Handle workflow not found exceptions.
     */
    @ExceptionHandler(WorkflowNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleWorkflowNotFoundException(
            WorkflowNotFoundException ex,
            HttpServletRequest request) {

        String correlationId = generateCorrelationId();

        log.warn("Workflow not found - correlationId: {}, message: {}",
                correlationId, ex.getMessage());

        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.NOT_FOUND.value())
                .error("WORKFLOW_NOT_FOUND")
                .message(ex.getMessage())
                .path(request.getRequestURI())
                .correlationId(correlationId)
                .build();

        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
    }

    /**
     * Handle workflow validation exceptions.
     */
    @ExceptionHandler(WorkflowValidationException.class)
    public ResponseEntity<ErrorResponse> handleWorkflowValidationException(
            WorkflowValidationException ex,
            HttpServletRequest request) {

        String correlationId = generateCorrelationId();

        log.warn("Workflow validation failed - correlationId: {}, message: {}",
                correlationId, ex.getMessage());

        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.BAD_REQUEST.value())
                .error("WORKFLOW_VALIDATION_ERROR")
                .message(ex.getMessage())
                .path(request.getRequestURI())
                .correlationId(correlationId)
                .build();

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    /**
     * Handle validation errors from @Valid annotation.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(
            MethodArgumentNotValidException ex,
            HttpServletRequest request) {

        String correlationId = generateCorrelationId();
        BindingResult bindingResult = ex.getBindingResult();

        List<ErrorResponse.FieldError> fieldErrors = bindingResult.getFieldErrors().stream()
                .map(fieldError -> ErrorResponse.FieldError.builder()
                        .field(fieldError.getField())
                        .message(fieldError.getDefaultMessage())
                        .rejectedValue(fieldError.getRejectedValue())
                        .build())
                .collect(Collectors.toList());

        log.warn("Validation failed - correlationId: {}, errors: {}",
                correlationId, fieldErrors.size());

        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.BAD_REQUEST.value())
                .error("VALIDATION_ERROR")
                .message("Validation failed for request")
                .path(request.getRequestURI())
                .correlationId(correlationId)
                .fieldErrors(fieldErrors)
                .build();

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    /**
     * Handle all other unexpected exceptions.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(
            Exception ex,
            HttpServletRequest request) {

        String correlationId = generateCorrelationId();

        log.error("Unexpected error - correlationId: {}, message: {}",
                correlationId, ex.getMessage(), ex);

        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.INTERNAL_SERVER_ERROR.value())
                .error("INTERNAL_SERVER_ERROR")
                .message("An unexpected error occurred")
                .path(request.getRequestURI())
                .correlationId(correlationId)
                .build();

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
    }

    /**
     * Generate a unique correlation ID for error tracking.
     */
    private String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }
}
