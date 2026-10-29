package lab.admin;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import lab.step3_outbox.Step3OrderConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class AdminController {

    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;

    @Autowired(required = false)
    private Step3OrderConsumer step3OrderConsumer;

    /**
     * 모든 테이블 데이터를 비우고 초기 테스트 데이터를 적재합니다.
     */
    @PostMapping("/reset")
    @Transactional
    public ResponseEntity<Map<String, Object>> reset() {
        log.info("[Admin] Resetting database tables and seed data...");

        // 1. PostgreSQL 16 CASCADE TRUNCATE
        jdbcTemplate.execute("TRUNCATE TABLE orders, processed_message, outbox_event, product, user_point RESTART IDENTITY CASCADE");

        // 2. 초기 시드 데이터 적재
        jdbcTemplate.update("INSERT INTO product (id, stock) VALUES (?, ?)", 1L, 1000);
        jdbcTemplate.update("INSERT INTO product (id, stock) VALUES (?, ?)", 2L, 1000);

        jdbcTemplate.update("INSERT INTO user_point (user_id, balance) VALUES (?, ?)", 1L, 10000L);
        jdbcTemplate.update("INSERT INTO user_point (user_id, balance) VALUES (?, ?)", 2L, 10000L);

        // 3. Step 3 컨슈머 카운터 리셋
        if (step3OrderConsumer != null) {
            step3OrderConsumer.reset();
        }

        log.info("[Admin] Reset completed successfully.");
        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "message", "Database reset and initial seed data inserted.",
                "initialProducts", Map.of(1L, 1000, 2L, 1000),
                "initialUserPoints", Map.of(1L, 10000L, 2L, 10000L)
        ));
    }

    /**
     * 현재 HikariCP 커넥션 풀 상태(Active, Idle, Pending Threads 등)를 조회합니다.
     */
    @GetMapping("/metrics")
    public ResponseEntity<HikariMetricsDto> getMetrics() {
        HikariPoolMXBean mxBean = getHikariPoolMXBean();

        if (mxBean == null) {
            return ResponseEntity.ok(new HikariMetricsDto(0, 0, 0, 0, 10));
        }

        HikariMetricsDto metrics = new HikariMetricsDto(
                mxBean.getActiveConnections(),
                mxBean.getIdleConnections(),
                mxBean.getTotalConnections(),
                mxBean.getThreadsAwaitingConnection(),
                10
        );

        return ResponseEntity.ok(metrics);
    }

    private HikariPoolMXBean getHikariPoolMXBean() {
        try {
            if (dataSource.isWrapperFor(HikariDataSource.class)) {
                HikariDataSource hikariDataSource = dataSource.unwrap(HikariDataSource.class);
                return hikariDataSource.getHikariPoolMXBean();
            }
        } catch (SQLException e) {
            log.warn("[Admin] Failed to unwrap HikariDataSource", e);
        }
        return null;
    }

    public record HikariMetricsDto(
            int activeConnections,
            int idleConnections,
            int totalConnections,
            int threadsAwaitingConnection,
            int maxPoolSize
    ) {}
}
