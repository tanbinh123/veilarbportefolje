package no.nav.pto.veilarbportefolje.postgres.sort;

import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class SortJoiner {
    private final String prefix;
    private final String delimiter;
    private final String suffix;

    private List<SortElement> sortElements = new ArrayList();

    public void add(String sortField, SortOrder sortOrder) {
        sortElements.add(new SortElement(sortField, sortOrder));
    }


    public String toString() {
        return sortElements
                .stream()
                .map(s -> s.getSortColumn() + " " + s.getSortOrder())
                .collect(Collectors.joining(delimiter, prefix, suffix));
    }
}
