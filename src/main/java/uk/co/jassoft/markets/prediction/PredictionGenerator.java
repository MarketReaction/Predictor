package uk.co.jassoft.markets.prediction;

import uk.co.jassoft.markets.datamodel.Direction;
import uk.co.jassoft.markets.datamodel.company.Company;
import uk.co.jassoft.markets.datamodel.company.quote.Quote;
import uk.co.jassoft.markets.datamodel.company.sentiment.StorySentiment;
import uk.co.jassoft.markets.datamodel.learningmodel.LearningModelRecord;
import uk.co.jassoft.markets.datamodel.prediction.Prediction;
import uk.co.jassoft.markets.datamodel.prediction.PredictionBuilder;
import uk.co.jassoft.markets.datamodel.system.Topic;
import uk.co.jassoft.markets.exceptions.quote.QuotePriceCalculationException;
import uk.co.jassoft.markets.exceptions.sentiment.SentimentException;
import uk.co.jassoft.markets.repository.*;
import uk.co.jassoft.markets.utils.QuoteUtils;
import uk.co.jassoft.markets.utils.SentimentUtil;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by jonshaw on 08/12/2015.
 */
@Component
public class PredictionGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(PredictionGenerator.class);

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private StorySentimentRepository storySentimentRepository;

    @Autowired
    private QuoteRepository quoteRepository;

    @Autowired
    private LearningModelRepository learningModelRepository;

    @Autowired
    private PredictionRepository predictionRepository;

    @Autowired
    private JmsTemplate jmsTemplate;

    private void predictionGenerated(final String message)
    {
        jmsTemplate.convertAndSend(Topic.PredictionGenerated.toString(), message);
    }

    public void generatePrediction(String companyId) {

        try {
            Company company = companyRepository.findOne(companyId);

            LOG.info("Prediction Generator running for company [{}] [{}]", company.getId(), company.getName());

            // get quote price change
            // TODO: use intraday where possible to determine how long prediction should last
            final List<Quote> quotes = quoteRepository.findByCompanyAndIntraday(company.getId(), false, new PageRequest(0, 7, new Sort(Sort.Direction.ASC, "date")));

            final List<StorySentiment> storySentiments = storySentimentRepository.findByCompany(company.getId());

            if (quotes.isEmpty()) {
                return;
            }

            final Quote lastQuote = quotes.get(quotes.size() - 1);

            List<LearningModelRecord> learningModelRecords = learningModelRepository.findByCompanyAndPreviousQuoteDirectionAndPreviousSentimentDirection(company.getId(),
                    QuoteUtils.getPreviousPriceDirection(quotes),
                    SentimentUtil.getPreviousSentimentDirection(storySentiments, lastQuote.getDate()));

            Double sentimentDifference = SentimentUtil.getLastSentimentDifferenceFromAverage(storySentiments, lastQuote.getDate());

            //Average quote change below difference
            // TODO: Should the filter abs the value first? to take into account negatives before doing < filter
            OptionalDouble averageQuoteChangeBelowValue = learningModelRecords
                    .stream()
                    .filter(isWithinDifferenceFromAverage(sentimentDifference))
                    .mapToDouble(value -> value.getResultingQuoteChange())
                    .average();
            OptionalDouble maxQuoteChangeBelowValue = learningModelRecords
                    .stream()
                    .filter(isWithinDifferenceFromAverage(sentimentDifference))
                    .mapToDouble(value -> value.getResultingQuoteChange())
                    .max();

            //Average quote change above difference
            OptionalDouble averageQuoteChangeAboveValue = learningModelRecords
                    .stream()
                    .filter(isWithinDifferenceFromAverage(sentimentDifference))
                    .mapToDouble(value -> value.getResultingQuoteChange())
                    .average();

            List<Double> valuesToAverage = new ArrayList<>();

            if(averageQuoteChangeBelowValue.isPresent())
                valuesToAverage.add(averageQuoteChangeBelowValue.getAsDouble());

            if(maxQuoteChangeBelowValue.isPresent())
                valuesToAverage.add(maxQuoteChangeBelowValue.getAsDouble());

            if(averageQuoteChangeAboveValue.isPresent())
                valuesToAverage.add(averageQuoteChangeAboveValue.getAsDouble());

            if(valuesToAverage.isEmpty()) {
                LOG.info("Not enough Quote data to predict average change");
                return;
            }

            Double predictedQuoteChange = valuesToAverage.stream().mapToDouble(value -> value).average().getAsDouble();

            Double predictedQuoteChangePercent = (predictedQuoteChange / lastQuote.getClose()) * 100; // Change / Last * 100

            Direction direction = Direction.None;

            if(predictedQuoteChange > 0)
                direction = Direction.Up;

            if(predictedQuoteChange < 0)
                direction = Direction.Down;

            DateTime predictionDate = new DateTime();

            DateTime endDate = predictionDate.plusDays(1);

            if(endDate.getDayOfWeek() == 6) {
                endDate = endDate.plusDays(2);
            }

            if(endDate.getDayOfWeek() == 7) {
                endDate = endDate.plusDays(1);
            }

            Page<Prediction> predictionsForCompany = predictionRepository.findByCompany(company.getId(), new PageRequest(0, 100));

            List<Prediction> matchingPredictions = predictionsForCompany.getContent().stream().filter(isValidated()).filter(matchesDirection(direction)).filter(isWithinLastDays(30)).collect(Collectors.toList());

            List<Prediction> correctPredictions = matchingPredictions.stream().filter(isCorrect()).collect(Collectors.toList());

            Double certainty = 0.5;

            if(!matchingPredictions.isEmpty()) {
                certainty = (double) correctPredictions.size() / (double) matchingPredictions.size();
            }

            if(certainty == 1 && matchingPredictions.size() < 3) {
                certainty = 0.60;
            }

            double potentialEarningPerShare = Math.abs(lastQuote.getBid() - (lastQuote.getAsk() - predictedQuoteChange));

            LOG.info("Correct predictions [{}] /  Matching predictions [{}]. Resulting in certainty of [{}]", matchingPredictions.size(), correctPredictions.size(), certainty);

            Prediction prediction = PredictionBuilder.aPrediction()
                    .withCompany(company.getId())
                    .withPredictionDate(predictionDate.toDate())
                    .withValidityPeriod(endDate.getMillis() - predictionDate.getMillis())
                    .withCertainty(certainty)
                    .withPredictedChange(predictedQuoteChange)
                    .withPredictedChangePercent(predictedQuoteChangePercent)
                    .withDirection(direction)
                    .withLastBid(lastQuote.getBid())
                    .withLastAsk(lastQuote.getAsk())
                    .withPotentialEarningPerShare(potentialEarningPerShare)
                    .build();

            List<Prediction> openPredictionsForCompany = predictionRepository.findByCompanyAndCorrectIsNull(company.getId());

            if(!openPredictionsForCompany.isEmpty()) {
                for(Prediction openPrediction : openPredictionsForCompany) {
                    if(openPrediction.getDirection().equals(prediction.getDirection())
                            && openPrediction.getPredictedChange().equals(prediction.getPredictedChange())) {
                        if(openPrediction.getCertainty().equals(prediction.getCertainty())) {
                            LOG.info("Duplicate Prediction generated for company [{}] - Ignoring prediction", company.getName());
                            return;
                        }
                        else {
                            openPrediction.setCertainty(prediction.getCertainty());
                            predictionRepository.save(openPrediction);
                            LOG.info("Duplicate Prediction generated for company [{}] with different Certainty - Updating prediction", company.getName());
                            return;
                        }
                    }
                }
            }

            LOG.info("Prediction Generated for company [{}] [{}]", company.getId(), company.getName());
            predictionRepository.save(prediction);
            predictionGenerated(prediction.getId());

        }
        catch (QuotePriceCalculationException | SentimentException exception) {
            LOG.info(exception.getLocalizedMessage());
        }
        catch (final Exception exception)
        {
            LOG.error(exception.getLocalizedMessage(), exception);

            throw new RuntimeException(exception);
        }
    }

    public static Predicate<Prediction> isValidated() {
        return prediction -> prediction.getCorrect() != null;
    }

    public static Predicate<Prediction> isWithinLastDays(int daysInPast) {
        return prediction -> new DateTime().minusDays(daysInPast).isBefore(prediction.getPredictionDate().getTime());
    }

    public static Predicate<Prediction> matchesDirection(Direction directionToMatch) {
        return prediction -> prediction.getDirection().equals(directionToMatch);
    }

    public static Predicate<Prediction> isCorrect() {
        return prediction -> prediction.getCorrect();
    }

    public static Predicate<LearningModelRecord> isWithinDifferenceFromAverage(Double sentimentDifference) {
        return learningModelRecord1 -> learningModelRecord1.getLastSentimentDifferenceFromAverage() < sentimentDifference;
    }
}
