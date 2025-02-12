package no.nav.pto.veilarbportefolje.config;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import static no.nav.pto.veilarbportefolje.util.DbUtils.createDataSource;
import static no.nav.pto.veilarbportefolje.util.DbUtils.getSqlAdminRole;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableTransactionManagement
public class DbConfigPostgres implements DatabaseConfig{
    private final EnvironmentProperties environmentProperties;

    @Bean("Postgres")
    @Override
    public DataSource dataSource() {
        return createDataSource(environmentProperties.getDbUrl(), true);
    }

    @Bean("PostgresReadOnly")
    public DataSource dataSourceRead() {
        return createDataSource(environmentProperties.getDbUrl(), false);
    }

    @Bean(name="PostgresJdbc")
    @Override
    public JdbcTemplate db(@Qualifier("Postgres") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name="PostgresJdbcReadOnly")
    public JdbcTemplate dbRead(@Qualifier("PostgresReadOnly") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean("PostgresNamedJdbc")
    @Override
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate(@Qualifier("Postgres")DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name="PostgresNamedJdbcReadOnly")
    public NamedParameterJdbcTemplate namedParameterJdbcTemplateRead(@Qualifier("PostgresReadOnly") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean("PostgresTransactionManager")
    @Override
    public PlatformTransactionManager transactionManager(@Qualifier("Postgres") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @PostConstruct
    @SneakyThrows
    public void migrateDb() {
        log.info("Starting database migration...");
        DataSource dataSource = createDataSource(environmentProperties.getDbUrl(), true);

        Flyway.configure()
                .dataSource(dataSource)
                .locations("db/postgres")
                .initSql("SET ROLE '" + getSqlAdminRole()+"';")
                .baselineOnMigrate(true)
                .load()
                .migrate();

        dataSource.getConnection().close();
    }
}
