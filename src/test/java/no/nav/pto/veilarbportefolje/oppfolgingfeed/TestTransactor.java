package no.nav.pto.veilarbportefolje.oppfolgingfeed;

import no.nav.common.utils.ExceptionUtils;
import no.nav.pto.veilarbportefolje.database.Transactor;

class TestTransactor extends Transactor {

    public TestTransactor() {
        super(null);
    }

    @Override
    public void inTransaction(InTransaction inTransaction) {
        try {
            inTransaction.run();
        } catch (Throwable throwable) {
            ExceptionUtils.throwUnchecked(throwable);
        }
    }

}
