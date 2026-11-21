package com.shop.etl;

import com.shop.etl.common.config.EtlProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Aakhi application no START point.
 *
 * Aa ek j Spring Boot application 3 alag rite job chalavi shake che:
 *   1. API       : POST /api/jobs/J2_SALES/runs           (JobController)
 *   2. Airflow   : API call kare, ke pachhi CLI mode ma jar chalave  (CliJobRunner)
 *   3. Scheduler : application ni andar cron                (JobScheduler)
 *
 * Tranne rasta chhelle EK j class JobLauncher ne call kare che. Etle lock, audit ane watermark
 * no logic ek j jagyae che, ane trigger koi pan hoy, job hammesha same rite j chale che.
 *
 * {@code @EnableScheduling}: Spring na scheduler ne chalu kare che. Kayo job kyare chalse e
 * JobScheduler config (yml) mathi nakki kare che. Prod ma aa mostly bandh j rakhvu (Airflow chalave).
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(EtlProperties.class)
public class ShopEtlApplication {

    /**
     * Java program no main method. Spring context banave che: config vanche che, SparkSession,
     * DataSource, jobs ane triggers ready kare che.
     *
     * CLI mode (Airflow mate) ma {@code --etl.cli.job=J2_SALES} aapo to job chali ne process
     * pote bandh thai jay che (CliJobRunner jovo).
     */
    public static void main(String[] args) {
        SpringApplication.run(ShopEtlApplication.class, args);
    }
}
