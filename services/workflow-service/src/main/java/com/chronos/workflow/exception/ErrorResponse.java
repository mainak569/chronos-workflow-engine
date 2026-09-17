package com.chronos.workflow.exception;

import java.time.Instant;
import java.util.List;

/**
 * Standard error response structure for API errors.
 */
public class ErrorResponse {

    private Instant timestamp;
    private int status;
    private String error;
    private String message;
    private String path;
    private String correlationId;
    private List<FieldError> fieldErrors;
    
    // Constructors
    public ErrorResponse() {
    }
    
    public ErrorResponse(Instant timestamp, int status, String error, String message, 
                        String path, String correlationId, List<FieldError> fieldErrors) {
        this.timestamp = timestamp;
        this.status = status;
        this.error = error;
        this.message = message;
        this.path = path;
        this.correlationId = correlationId;
        this.fieldErrors = fieldErrors;
    }
    
    // Getters and Setters
    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public List<FieldError> getFieldErrors() {
        return fieldErrors;
    }

    public void setFieldErrors(List<FieldError> fieldErrors) {
        this.fieldErrors = fieldErrors;
    }
    
    // Builder
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private Instant timestamp;
        private int status;
        private String error;
        private String message;
        private String path;
        private String correlationId;
        private List<FieldError> fieldErrors;
        
        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        public Builder status(int status) {
            this.status = status;
            return this;
        }
        
        public Builder error(String error) {
            this.error = error;
            return this;
        }
        
        public Builder message(String message) {
            this.message = message;
            return this;
        }
        
        public Builder path(String path) {
            this.path = path;
            return this;
        }
        
        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }
        
        public Builder fieldErrors(List<FieldError> fieldErrors) {
            this.fieldErrors = fieldErrors;
            return this;
        }
        
        public ErrorResponse build() {
            return new ErrorResponse(timestamp, status, error, message, path, correlationId, fieldErrors);
        }
    }

    public static class FieldError {
        private String field;
        private String message;
        private Object rejectedValue;
        
        // Constructors
        public FieldError() {
        }
        
        public FieldError(String field, String message, Object rejectedValue) {
            this.field = field;
            this.message = message;
            this.rejectedValue = rejectedValue;
        }
        
        // Getters and Setters
        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public Object getRejectedValue() {
            return rejectedValue;
        }

        public void setRejectedValue(Object rejectedValue) {
            this.rejectedValue = rejectedValue;
        }
        
        // Builder
        public static Builder builder() {
            return new Builder();
        }
        
        public static class Builder {
            private String field;
            private String message;
            private Object rejectedValue;
            
            public Builder field(String field) {
                this.field = field;
                return this;
            }
            
            public Builder message(String message) {
                this.message = message;
                return this;
            }
            
            public Builder rejectedValue(Object rejectedValue) {
                this.rejectedValue = rejectedValue;
                return this;
            }
            
            public FieldError build() {
                return new FieldError(field, message, rejectedValue);
            }
        }
    }
}
