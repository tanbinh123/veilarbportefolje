package no.nav.pto.veilarbportefolje.postgres.sort;

import no.nav.pto.veilarbportefolje.domene.Filtervalg;

import java.util.Objects;

import static no.nav.pto.veilarbportefolje.database.PostgresTable.BRUKER_VIEW.*;

public class PostgresSortQueryBuilder {
    private final SortJoiner sortStatement = new SortJoiner(" ORDER BY ", ",", ";");

    public void createSortStatements(String sortField, SortOrder order, Filtervalg filtervalg, boolean kallesFraMinOversikt) {
        if (Objects.isNull(sortField) || sortField.equals("ikke_satt")) {
            sortStatement.add(AKTOERID, SortOrder.ASC);
            return;
        if (kallesFraMinOversikt) {
            sortStatement.add(NY_FOR_VEILEDER, SortOrder.DESC);
        }

        switch (sortField) {
            case "aapmaxtiduke":
                break;
            case "aaprettighetsperiode":
                break;
            case "aapunntakukerigjen":
                break;
            case "aktivitet_start":
                break;
            case "ansvarlig_veileder_for_vedtak":
                sortStatement.add(VEDTAKSTATUS_ANSVARLIG_VEILDERNAVN, order);
                break;
            case "arbeidsliste_overskrift":
                break;
            case "arbeidslistefrist":
                sortStatement.add(ARB_FRIST, order);
                break;
            case "arbeidslistekategori":
                sortStatement.add(ARB_KATEGORI, order);
                break;
            case "dagputlopuke":
                break;
            case "etternavn":
                sortStatement.add(ETTERNAVN, order);
                break;
            case "fodselsnummer":
                sortStatement.add(FODSELSNR, order);
                break;
            case "forrige_aktivitet_start":
                break;
            case "iavtaltaktivitet":
                break;
            case "moterMedNAVIdag":
                break;
            case "neste_aktivitet_start":
                break;
            case "oppfolging_startdato":
                sortStatement.add(STARTDATO, order);
                break;
            case "permutlopuke":
                break;
            case "siste_endring_tidspunkt":
                break;
            case "utlopsdato":
                break;
            case "utlopteaktiviteter":
                break;
            case "valgteaktiviteter":
                break;
            case "vedtak_status_endret":
                //@todo: is it necessary to convert this field from timestamp to string
                sortStatement.add(VEDTAKSTATUS_ENDRET_TIDSPUNKT, order);
                break;
            case "vedtakstatus":
                sortStatement.add(VEDTAKSTATUS, order);
                break;
            case "veileder_id":
                sortStatement.add(VEILEDERID, order);
                break;
            case "venterpasvarfrabruker":

                break;
            case "venterpasvarfranav":
                break;
        }

        addSecondarySort();
    }

    private void addSecondarySort() {
        sortStatement.add(AKTOERID, SortOrder.ASC);
    }

    public String getSortStatement() {
        return sortStatement.toString();
    }
}
