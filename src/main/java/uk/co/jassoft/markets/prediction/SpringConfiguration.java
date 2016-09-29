package uk.co.jassoft.markets.prediction;

import uk.co.jassoft.markets.BaseSpringConfiguration;
import uk.co.jassoft.markets.datamodel.prediction.PredictorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Created by jonshaw on 13/07/15.
 */
@Configuration
@ComponentScan("uk.co.jassoft.markets.prediction")
public class SpringConfiguration extends BaseSpringConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(SpringConfiguration.class);

    public static void main(String[] args) throws Exception {

        ConfigurableApplicationContext context = SpringApplication.run(SpringConfiguration.class, args);

        PredictorType predictorType = PredictorType.valueOf(args[0]);

        LOG.info("Running Predictor for Type [{}] Args [{}]", predictorType, args);

        switch (predictorType) {
            case PredictionValidator:
                context.getBean(PredictionValidator.class).validatePredictions();
                break;

            case PredictionGenerator:
                context.getBean(PredictionGenerator.class).generatePrediction(args[1]);
                break;
        }

        context.close();
        System.exit(0);
    }
}
