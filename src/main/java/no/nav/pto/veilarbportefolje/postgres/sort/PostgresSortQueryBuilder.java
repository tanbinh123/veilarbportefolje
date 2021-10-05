package no.nav.pto.veilarbportefolje.postgres.sort;

import no.nav.pto.veilarbportefolje.domene.Filtervalg;
import no.nav.pto.veilarbportefolje.util.ValideringsRegler;

import static no.nav.pto.veilarbportefolje.database.PostgresTable.BRUKER_VIEW.*;

public class PostgresSortQueryBuilder {
    private final SortJoiner sortStatement = new SortJoiner(" ORDER BY ", ",", ";");

    public void createSortStatements(String sortField, SortOrder order, Filtervalg filtervalg, boolean kallesFraMinOversikt) {
        if ("ikke_satt".equals(sortField)) {
            sortStatement.add(AKTOERID, SortOrder.ASC);
        }
        if (kallesFraMinOversikt) {
            sortStatement.add(NY_FOR_VEILEDER, SortOrder.DESC);
        }

        switch (sortField) {
            case "valgteaktiviteter":
                //sorterValgteAktiviteter(filtervalg, searchSourceBuilder, order);
                break;
            case "moterMedNAVIdag":
                //sortStatement.add("aktivitet_mote_startdato", order);
                break;
            case "iavtaltaktivitet":
                /*FieldSortBuilder builder = new FieldSortBuilder("aktivitet_utlopsdatoer")
                        .order(order)
                        .sortMode(MIN);
                */
                break;
            case "fodselsnummer":
                sortStatement.add(FODSELSNR, order);
                break;
            case "utlopteaktiviteter":
                //searchSourceBuilder.sort("nyesteutlopteaktivitet", order);
                break;
            case "arbeidslistefrist":
                //searchSourceBuilder.sort("arbeidsliste_frist", order);
                break;
            case "aaprettighetsperiode":
                //sorterAapRettighetsPeriode(searchSourceBuilder, order);
                break;
            case "vedtakstatus":
                sortStatement.add(VEDTAKSTATUS, order);
                break;
            case "arbeidslistekategori":
                //searchSourceBuilder.sort("arbeidsliste_kategori", order);
                break;
            case "siste_endring_tidspunkt":
                //sorterSisteEndringTidspunkt(searchSourceBuilder, order, filtervalg);
                break;
            default:
                defaultSort(sortField, order);
        }
        addSecondarySort();
    }

    private void defaultSort(String sortField, SortOrder order) {
        if (ValideringsRegler.sortFields.contains(sortField)) {
            //sortStatement.add(sortField, order);
        } else {
            throw new IllegalStateException();
        }
    }

    private void addSecondarySort() {
        sortStatement.add(AKTOERID, SortOrder.ASC);
    }

    public String getSortStatement() {
        return sortStatement.toString();
    }
}
