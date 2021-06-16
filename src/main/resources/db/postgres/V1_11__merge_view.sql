DROP VIEW BRUKER;
CREATE VIEW BRUKER AS
SELECT OD.AKTOERID,
       OD.OPPFOLGING,
       OD.STARTDATO,
       OD.NY_FOR_VEILEDER,
       OD.VEILEDERID,
       OD.MANUELL,
       OBA.KJONN,
       OBA.FODSELS_DATO,
       OBA.FODSELSNR,
       OBA.FORNAVN,
       OBA.ETTERNAVN,
       OBA.NAV_KONTOR,
       OBA.ISERV_FRA_DATO,
       OBA.FORMIDLINGSGRUPPEKODE,
       OBA.KVALIFISERINGSGRUPPEKODE,
       OBA.RETTIGHETSGRUPPEKODE,
       OBA.HOVEDMAALKODE,
       OBA.SIKKERHETSTILTAK_TYPE_KODE,
       OBA.DISKRESJONSKODE,
       OBA.HAR_OPPFOLGINGSSAK,
       OBA.SPERRET_ANSATT,
       OBA.ER_DOED,
       D.VENTER_PA_BRUKER,
       D.VENTER_PA_NAV,
       V.VEDTAKSTATUS,
       BP.PROFILERING_RESULTAT,
       V.ANSVARLIG_VEILDERNAVN          as VEDTAKSTATUS_ANSVARLIG_VEILDERNAVN,
       V.ENDRET_TIDSPUNKT               as VEDTAKSTATUS_ENDRET_TIDSPUNKT,
       ARB.SIST_ENDRET_AV_VEILEDERIDENT as ARB_SIST_ENDRET_AV_VEILEDERIDENT,
       ARB.ENDRINGSTIDSPUNKT            as ARB_ENDRINGSTIDSPUNKT,
       ARB.OVERSKRIFT                   as ARB_OVERSKRIFT,
       ARB.KOMMENTAR                    as ARB_KOMMENTAR,
       ARB.FRIST                        as ARB_FRIST,
       ARB.KATEGORI                     as ARB_KATEGORI

FROM OPPFOLGING_DATA OD
         LEFT JOIN OPPFOLGINGSBRUKER_ARENA OBA ON OBA.AKTOERID = OD.AKTOERID
         LEFT JOIN DIALOG D ON D.AKTOERID = OD.AKTOERID
         LEFT JOIN VEDTAKSTATUS V on V.AKTOERID = OD.AKTOERID
         LEFT JOIN ARBEIDSLISTE ARB on ARB.AKTOERID = OD.AKTOERID
         LEFT JOIN BRUKER_PROFILERING BP ON BP.AKTOERID = OD.AKTOERID;