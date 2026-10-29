package lab.step1_rdbms;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.NestedRuntimeException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> handleException(Exception ex) {
        String rootMsg = ex.getMessage();
        if (ex instanceof NestedRuntimeException nre && nre.getMostSpecificCause() != null) {
            rootMsg = nre.getMostSpecificCause().getMessage();
        }
        log.error("[Error Handler] Exception caught: {} - {}", ex.getClass().getSimpleName(), rootMsg);

        // bench.mjs 가 에러 유형(SQLTransientConnectionException, timed out, 40P01, deadlock)을 파싱할 수 있도록 에러 본문 전달
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("ERROR: " + ex.getClass().getName() + " - " + rootMsg);
    }
}
