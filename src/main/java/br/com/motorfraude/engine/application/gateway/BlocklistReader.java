package br.com.motorfraude.engine.application.gateway;

import br.com.motorfraude.contracts.EvaluateTransaction;

public interface BlocklistReader {

    ListFlags flagsFor(EvaluateTransaction transaction);

    record ListFlags(boolean counterpartyFlagged, boolean deviceFlagged) {

        public static final ListFlags NONE = new ListFlags(false, false);
    }
}
