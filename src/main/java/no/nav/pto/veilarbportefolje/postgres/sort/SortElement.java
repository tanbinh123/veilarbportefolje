package no.nav.pto.veilarbportefolje.postgres.sort;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SortElement {
    private final String sortColumn;
    private final SortOrder sortOrder;
}
