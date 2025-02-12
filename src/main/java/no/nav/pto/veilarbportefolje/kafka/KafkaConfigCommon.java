package no.nav.pto.veilarbportefolje.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import no.nav.arbeid.cv.avro.Melding;
import no.nav.arbeid.soker.profilering.ArbeidssokerProfilertEvent;
import no.nav.arbeid.soker.registrering.ArbeidssokerRegistrertEvent;
import no.nav.common.kafka.consumer.KafkaConsumerClient;
import no.nav.common.kafka.consumer.feilhandtering.KafkaConsumerRecordProcessor;
import no.nav.common.kafka.consumer.feilhandtering.KafkaConsumerRepository;
import no.nav.common.kafka.consumer.feilhandtering.util.KafkaConsumerRecordProcessorBuilder;
import no.nav.common.kafka.consumer.util.KafkaConsumerClientBuilder;
import no.nav.common.kafka.consumer.util.deserializer.Deserializers;
import no.nav.common.kafka.spring.PostgresJdbcTemplateConsumerRepository;
import no.nav.common.utils.Credentials;
import no.nav.common.utils.EnvironmentUtils;
import no.nav.pto.veilarbportefolje.aktiviteter.AktivitetService;
import no.nav.pto.veilarbportefolje.aktiviteter.KafkaAktivitetMelding;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.GruppeAktivitetService;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.TiltakServiceV2;
import no.nav.pto.veilarbportefolje.arenapakafka.aktiviteter.UtdanningsAktivitetService;
import no.nav.pto.veilarbportefolje.arenapakafka.arenaDTO.GruppeAktivitetDTO;
import no.nav.pto.veilarbportefolje.arenapakafka.arenaDTO.TiltakDTO;
import no.nav.pto.veilarbportefolje.arenapakafka.arenaDTO.UtdanningsAktivitetDTO;
import no.nav.pto.veilarbportefolje.arenapakafka.arenaDTO.YtelsesDTO;
import no.nav.pto.veilarbportefolje.arenapakafka.ytelser.TypeKafkaYtelse;
import no.nav.pto.veilarbportefolje.arenapakafka.ytelser.YtelsesService;
import no.nav.pto.veilarbportefolje.cv.CVService;
import no.nav.pto.veilarbportefolje.cv.dto.CVMelding;
import no.nav.pto.veilarbportefolje.dialog.DialogService;
import no.nav.pto.veilarbportefolje.dialog.Dialogdata;
import no.nav.pto.veilarbportefolje.opensearch.MetricsReporter;
import no.nav.pto.veilarbportefolje.kafka.deserializers.AivenAvroDeserializer;
import no.nav.pto.veilarbportefolje.kafka.deserializers.OnpremAvroDeserializer;
import no.nav.pto.veilarbportefolje.kafka.unleash.KafkaAivenUnleash;
import no.nav.pto.veilarbportefolje.kafka.unleash.KafkaOnpremUnleash;
import no.nav.pto.veilarbportefolje.mal.MalEndringKafkaDTO;
import no.nav.pto.veilarbportefolje.mal.MalService;
import no.nav.pto.veilarbportefolje.oppfolging.ManuellStatusDTO;
import no.nav.pto.veilarbportefolje.oppfolging.ManuellStatusService;
import no.nav.pto.veilarbportefolje.oppfolging.NyForVeilederDTO;
import no.nav.pto.veilarbportefolje.oppfolging.NyForVeilederService;
import no.nav.pto.veilarbportefolje.oppfolging.OppfolgingAvsluttetDTO;
import no.nav.pto.veilarbportefolje.oppfolging.OppfolgingAvsluttetService;
import no.nav.pto.veilarbportefolje.oppfolging.OppfolgingStartetDTO;
import no.nav.pto.veilarbportefolje.oppfolging.OppfolgingStartetService;
import no.nav.pto.veilarbportefolje.oppfolging.VeilederTilordnetDTO;
import no.nav.pto.veilarbportefolje.oppfolging.VeilederTilordnetService;
import no.nav.pto.veilarbportefolje.oppfolgingsbruker.OppfolginsbrukerService;
import no.nav.pto.veilarbportefolje.profilering.ProfileringService;
import no.nav.pto.veilarbportefolje.registrering.RegistreringService;
import no.nav.pto.veilarbportefolje.service.UnleashService;
import no.nav.pto.veilarbportefolje.sistelest.SistLestKafkaMelding;
import no.nav.pto.veilarbportefolje.sistelest.SistLestService;
import no.nav.pto.veilarbportefolje.vedtakstotte.KafkaVedtakStatusEndring;
import no.nav.pto.veilarbportefolje.vedtakstotte.VedtakService;
import no.nav.pto_schema.kafka.json.topic.onprem.EndringPaaOppfoelgingsBrukerV2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static no.nav.common.kafka.consumer.util.ConsumerUtils.findConsumerConfigsWithStoreOnFailure;
import static no.nav.common.kafka.util.KafkaPropertiesPreset.aivenDefaultConsumerProperties;
import static no.nav.common.kafka.util.KafkaPropertiesPreset.onPremDefaultConsumerProperties;
import static no.nav.common.utils.EnvironmentUtils.isDevelopment;
import static no.nav.common.utils.NaisUtils.getCredentials;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

@Configuration
public class KafkaConfigCommon {
    public final static String CLIENT_ID_CONFIG = "veilarbportefolje-consumer";
    public static final String KAFKA_BROKERS = EnvironmentUtils.getRequiredProperty("KAFKA_BROKERS_URL");
    private static final Credentials serviceUserCredentials = getCredentials("service_user");
    private static final String KAFKA_SCHEMAS_URL = EnvironmentUtils.getRequiredProperty("KAFKA_SCHEMAS_URL");

    public enum Topic {
        VEDTAK_STATUS_ENDRING_TOPIC("pto.vedtak-14a-statusendring-v1"),
        DIALOG_CONSUMER_TOPIC("aapen-fo-endringPaaDialog-v1-" + requireKafkaTopicPostfix()),
        ENDRING_PAA_MANUELL_STATUS("aapen-arbeidsrettetOppfolging-endringPaManuellStatus-v1-" + requireKafkaTopicPostfix()),
        VEILEDER_TILORDNET("aapen-arbeidsrettetOppfolging-veilederTilordnet-v1-" + requireKafkaTopicPostfix()),
        ENDRING_PAA_NY_FOR_VEILEDER("aapen-arbeidsrettetOppfolging-endringPaNyForVeileder-v1-" + requireKafkaTopicPostfix()),
        OPPFOLGING_STARTET("aapen-arbeidsrettetOppfolging-oppfolgingStartet-v1-" + requireKafkaTopicPostfix()),
        OPPFOLGING_AVSLUTTET("aapen-arbeidsrettetOppfolging-oppfolgingAvsluttet-v1-" + requireKafkaTopicPostfix()),
        ENDRING_PA_MAL("aapen-arbeidsrettetOppfolging-endringPaMal-v1-" + requireKafkaTopicPostfix()),
        SIST_LEST("aapen-fo-veilederHarLestAktivitetsplanen-v1"),
        ENDRING_PAA_OPPFOLGINGSBRUKER("pto.endring-paa-oppfolgingsbruker-v2"),
        CV_ENDRET("arbeid-pam-cv-endret-v6"),
        CV_TOPIC("teampam.samtykke-status-1"),

        AIVEN_REGISTRERING_TOPIC("paw.arbeidssoker-registrert-v1"),
        AIVEN_PROFILERING_TOPIC("paw.arbeidssoker-profilert-v1"),

        AIVEN_AKTIVITER_TOPIC("pto.aktivitet-portefolje-v1"),

        TILTAK_TOPIC("teamarenanais.aapen-arena-tiltaksaktivitetendret-v1-" + requireKafkaTopicPostfix()),
        UTDANNINGS_AKTIVITET_TOPIC("teamarenanais.aapen-arena-utdanningsaktivitetendret-v1-" + requireKafkaTopicPostfix()),
        GRUPPE_AKTIVITET_TOPIC("teamarenanais.aapen-arena-gruppeaktivitetendret-v1-" + requireKafkaTopicPostfix()),
        AAP_TOPIC("teamarenanais.aapen-arena-aapvedtakendret-v1-" + requireKafkaTopicPostfix()),
        DAGPENGE_TOPIC("teamarenanais.aapen-arena-dagpengevedtakendret-v1-" + requireKafkaTopicPostfix()),
        TILTAKSPENGER_TOPIC("teamarenanais.aapen-arena-tiltakspengevedtakendret-v1-" + requireKafkaTopicPostfix());

        @Getter
        final String topicName;

        Topic(String topicName) {
            this.topicName = topicName;
        }
    }

    private final List<KafkaConsumerClient> consumerClientAiven;
    private final List<KafkaConsumerClient> consumerClientsOnPrem;
    private final KafkaConsumerRecordProcessor consumerRecordProcessor;

    public KafkaConfigCommon(CVService cvService,
                             SistLestService sistLestService, RegistreringService registreringService,
                             ProfileringService profileringService, AktivitetService aktivitetService,
                             VedtakService vedtakService, DialogService dialogService, OppfolgingStartetService oppfolgingStartetService,
                             OppfolgingAvsluttetService oppfolgingAvsluttetService, ManuellStatusService manuellStatusService,
                             NyForVeilederService nyForVeilederService, VeilederTilordnetService veilederTilordnetService,
                             MalService malService, OppfolginsbrukerService oppfolginsbrukerService, TiltakServiceV2 tiltakServiceV2,
                             UtdanningsAktivitetService utdanningsAktivitetService, GruppeAktivitetService gruppeAktivitetService,
                             YtelsesService ytelsesService, @Qualifier("PostgresJdbc") JdbcTemplate jdbcTemplate,
                             UnleashService unleashService) {
        KafkaConsumerRepository consumerRepository = new PostgresJdbcTemplateConsumerRepository(jdbcTemplate);
        MeterRegistry prometheusMeterRegistry = new MetricsReporter.ProtectedPrometheusMeterRegistry();

        List<KafkaConsumerClientBuilder.TopicConfig<?, ?>> topicConfigsAiven =
                List.of(new KafkaConsumerClientBuilder.TopicConfig<String, CVMelding>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.CV_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(CVMelding.class),
                                        cvService::behandleKafkaMeldingCVHjemmel
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, UtdanningsAktivitetDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.UTDANNINGS_AKTIVITET_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(UtdanningsAktivitetDTO.class),
                                        utdanningsAktivitetService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, GruppeAktivitetDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.GRUPPE_AKTIVITET_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(GruppeAktivitetDTO.class),
                                        gruppeAktivitetService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, TiltakDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.TILTAK_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(TiltakDTO.class),
                                        tiltakServiceV2::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, ArbeidssokerRegistrertEvent>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.AIVEN_REGISTRERING_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        new AivenAvroDeserializer<ArbeidssokerRegistrertEvent>().getDeserializer(),
                                        registreringService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, ArbeidssokerProfilertEvent>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.AIVEN_PROFILERING_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        new AivenAvroDeserializer<ArbeidssokerProfilertEvent>().getDeserializer(),
                                        profileringService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, YtelsesDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.AAP_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(YtelsesDTO.class),
                                        (melding -> {
                                            ytelsesService.behandleKafkaRecord(melding, TypeKafkaYtelse.AAP);
                                        })
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, YtelsesDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.DAGPENGE_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(YtelsesDTO.class),
                                        (melding -> {
                                            ytelsesService.behandleKafkaRecord(melding, TypeKafkaYtelse.DAGPENGER);
                                        })
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, YtelsesDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.TILTAKSPENGER_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(YtelsesDTO.class),
                                        (melding -> {
                                            ytelsesService.behandleKafkaRecord(melding, TypeKafkaYtelse.TILTAKSPENGER);
                                        })
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, EndringPaaOppfoelgingsBrukerV2>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.ENDRING_PAA_OPPFOLGINGSBRUKER.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(EndringPaaOppfoelgingsBrukerV2.class),
                                        oppfolginsbrukerService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, KafkaAktivitetMelding>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.AIVEN_AKTIVITER_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(KafkaAktivitetMelding.class),
                                        aktivitetService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, KafkaVedtakStatusEndring>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.VEDTAK_STATUS_ENDRING_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(KafkaVedtakStatusEndring.class),
                                        vedtakService::behandleKafkaRecord
                                )
                );
        List<KafkaConsumerClientBuilder.TopicConfig<?, ?>> topicConfigsOnPrem =
                List.of(new KafkaConsumerClientBuilder.TopicConfig<String, SistLestKafkaMelding>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.SIST_LEST.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(SistLestKafkaMelding.class),
                                        sistLestService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, Dialogdata>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.DIALOG_CONSUMER_TOPIC.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(Dialogdata.class),
                                        dialogService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, OppfolgingStartetDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.OPPFOLGING_STARTET.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(OppfolgingStartetDTO.class),
                                        oppfolgingStartetService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, OppfolgingAvsluttetDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.OPPFOLGING_AVSLUTTET.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(OppfolgingAvsluttetDTO.class),
                                        oppfolgingAvsluttetService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, ManuellStatusDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.ENDRING_PAA_MANUELL_STATUS.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(ManuellStatusDTO.class),
                                        manuellStatusService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, NyForVeilederDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.ENDRING_PAA_NY_FOR_VEILEDER.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(NyForVeilederDTO.class),
                                        nyForVeilederService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, VeilederTilordnetDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.VEILEDER_TILORDNET.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(VeilederTilordnetDTO.class),
                                        veilederTilordnetService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, MalEndringKafkaDTO>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.ENDRING_PA_MAL.topicName,
                                        Deserializers.stringDeserializer(),
                                        Deserializers.jsonDeserializer(MalEndringKafkaDTO.class),
                                        malService::behandleKafkaRecord
                                ),
                        new KafkaConsumerClientBuilder.TopicConfig<String, Melding>()
                                .withLogging()
                                .withMetrics(prometheusMeterRegistry)
                                .withStoreOnFailure(consumerRepository)
                                .withConsumerConfig(
                                        Topic.CV_ENDRET.topicName,
                                        Deserializers.stringDeserializer(),
                                        new OnpremAvroDeserializer<Melding>().getDeserializer(),
                                        cvService::behandleKafkaRecord
                                )
                );

        KafkaAivenUnleash kafkaAivenUnleash = new KafkaAivenUnleash(unleashService);
        KafkaOnpremUnleash kafkaOnpremUnleash = new KafkaOnpremUnleash(unleashService);

        Properties aivenConsumerProperties = aivenDefaultConsumerProperties(CLIENT_ID_CONFIG);
        //aivenConsumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");
        aivenConsumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumerClientAiven = topicConfigsAiven.stream()
                .map(config ->
                        KafkaConsumerClientBuilder.builder()
                                .withProperties(aivenConsumerProperties)
                                .withTopicConfig(config)
                                .withToggle(kafkaAivenUnleash)
                                .build())
                .collect(Collectors.toList());

        consumerClientsOnPrem = topicConfigsOnPrem.stream()
                .map(config ->
                        KafkaConsumerClientBuilder.builder()
                                .withProperties(onPremDefaultConsumerProperties(
                                        CLIENT_ID_CONFIG,
                                        KAFKA_BROKERS,
                                        serviceUserCredentials)
                                )
                                .withTopicConfig(config)
                                .withToggle(kafkaOnpremUnleash)
                                .build())
                .collect(Collectors.toList());

        consumerRecordProcessor = KafkaConsumerRecordProcessorBuilder
                .builder()
                .withLockProvider(new JdbcTemplateLockProvider(jdbcTemplate))
                .withKafkaConsumerRepository(consumerRepository)
                .withConsumerConfigs(findConsumerConfigsWithStoreOnFailure(Stream.concat(topicConfigsAiven.stream(), topicConfigsOnPrem.stream()).collect(Collectors.toList())))
                .build();
    }


    @PostConstruct
    public void start() {
        consumerRecordProcessor.start();
        consumerClientAiven.forEach(KafkaConsumerClient::start);
        consumerClientsOnPrem.forEach(KafkaConsumerClient::start);
    }


    public static String requireKafkaTopicPostfix() {
        return isDevelopment().orElse(false) ? "q1" : "p";
    }
}
