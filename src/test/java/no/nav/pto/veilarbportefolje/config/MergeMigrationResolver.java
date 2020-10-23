package no.nav.pto.veilarbportefolje.config;

import org.flywaydb.core.api.resolver.BaseMigrationResolver;
import org.flywaydb.core.api.resolver.ResolvedMigration;
import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.DbSupportFactory;
import org.flywaydb.core.internal.resolver.ResolvedMigrationComparator;
import org.flywaydb.core.internal.resolver.sql.SqlMigrationResolver;
import org.flywaydb.core.internal.util.Location;
import org.flywaydb.core.internal.util.PlaceholderReplacer;
import org.flywaydb.core.internal.util.scanner.Scanner;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static no.nav.pto.veilarbportefolje.util.ExceptionUtils.sneakyThrows;

public class MergeMigrationResolver extends BaseMigrationResolver {
    private DbSupport dbSupport;
    private Scanner scanner;
    private PlaceholderReplacer placeholderReplacer;
    private ResolvedMigrationComparator comparator = new ResolvedMigrationComparator();

    private String[] mergeLocations = new String[]{"testmigration", "db/migration"};


    public void init() throws SQLException {
        scanner = new Scanner(Thread.currentThread().getContextClassLoader());
        placeholderReplacer = new PlaceholderReplacer(flywayConfiguration.getPlaceholders(),
                flywayConfiguration.getPlaceholderPrefix(),
                flywayConfiguration.getPlaceholderSuffix());
        dbSupport = DbSupportFactory.createDbSupport(flywayConfiguration.getDataSource().getConnection(), false);
    }

    @Override
    public List<ResolvedMigration> resolveMigrations() {
        sneakyThrows(() -> init());

        List<ResolvedMigration> migrations = new ArrayList<>();

        for (String locationStr : mergeLocations) {
            Location location = new Location(locationStr);
            SqlMigrationResolver res = new SqlMigrationResolver(dbSupport, scanner, location, placeholderReplacer,
                    "UTF-8", "V", "R", "__", ".sql");
            migrations.addAll(res.resolveMigrations());
        }

        return mergeDuplicateMigrations(migrations);
    }

    private List<ResolvedMigration> mergeDuplicateMigrations(List<ResolvedMigration> migrations){
        List<ResolvedMigration> newMigrationsList = new ArrayList<>();

        for (ResolvedMigration migration: migrations){
            if (!existsAllreadyInList(newMigrationsList, migration)){
                newMigrationsList.add(migration);
            }
        }

        return newMigrationsList;
    }

    private boolean existsAllreadyInList(List<ResolvedMigration> migrations, ResolvedMigration migration){
        for (ResolvedMigration possibleDuplicate: migrations){
            if (comparator.compare(possibleDuplicate, migration) == 0){
                return true;
            }
        }
        return false;
    }


}
