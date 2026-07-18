package io.github.viniciusdsandrade.outbox.payments;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

@Import(PostgresTestConfiguration.class)
@SpringBootTest
class PaymentsServiceApplicationTest {

    @Autowired
    private Flyway flyway;

    @Test
    void appliesBootstrapMigration() {
        assertThat(flyway.info().current()).isNotNull();
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("1");
    }
}
