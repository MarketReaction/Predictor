package uk.co.jassoft.markets.prediction;

import uk.co.jassoft.markets.datamodel.Direction;
import uk.co.jassoft.markets.datamodel.company.Company;
import uk.co.jassoft.markets.datamodel.company.Exchange;
import uk.co.jassoft.markets.datamodel.company.quote.Quote;
import uk.co.jassoft.markets.datamodel.prediction.Prediction;
import uk.co.jassoft.markets.datamodel.system.Queue;
import uk.co.jassoft.markets.repository.CompanyRepository;
import uk.co.jassoft.markets.repository.ExchangeRepository;
import uk.co.jassoft.markets.repository.PredictionRepository;
import uk.co.jassoft.markets.repository.QuoteRepository;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Predicate;

/**
 * Created by jonshaw on 08/12/2015.
 */
@Component
public class PredictionValidator {

    private static final Logger LOG = LoggerFactory.getLogger(PredictionValidator.class);

    @Autowired
    private PredictionRepository predictionRepository;

    @Autowired
    private QuoteRepository quoteRepository;

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private ExchangeRepository exchangeRepository;

    @Autowired
    private JmsTemplate jmsTemplate;

    void missingQuoteData(final Pair<String,Date> data)
    {
        jmsTemplate.convertAndSend(Queue.MissingQuoteData.toString(), data);
    }

    public void validatePredictions() {

        final Set<Pair<String,Date>> missingDatas = new HashSet<>();

        final List<Prediction> unValidatedPredictions = predictionRepository.findByCorrectIsNull();

        unValidatedPredictions.stream().filter(isOverdue()).forEach(prediction -> {

            DateTime startDate = new DateTime(prediction.getPredictionDate());

            if(startDate.getDayOfWeek() == 6) {
                startDate = startDate.minusDays(1);
            }

            if(startDate.getDayOfWeek() == 7) {
                startDate = startDate.minusDays(2);
            }

            final Company company = companyRepository.findOne(prediction.getCompany());
            final Exchange exchange = exchangeRepository.findOne(company.getExchange());

            Quote quoteAtPrediction = getQuoteAtDate(exchange, company, startDate.toDate());

            if(quoteAtPrediction == null) {
                LOG.debug("Quote at prediction not present for date [{}] Requesting retrieval", DateUtils.truncate(startDate.toDate(), Calendar.DATE));
                Date midnight = startDate.toDateMidnight().toDate();
                if(!missingDatas.contains(midnight)) {
                    missingDatas.add(new ImmutablePair<>(company.getExchange(), midnight));
                }
                return;
            }

            DateTime endDate = new DateTime(prediction.getPredictionDate()).plusMillis(prediction.getValidityPeriod().intValue());

            if(endDate.getDayOfWeek() == 6) {
                endDate = endDate.plusDays(2);
            }

            if(endDate.getDayOfWeek() == 7) {
                endDate = endDate.plusDays(1);
            }

            Quote quoteAtEndOfPrediction = getQuoteAtDate(exchange, company, endDate.toDate());

            if(quoteAtEndOfPrediction == null) {
                LOG.debug("Quote at end of prediction not present for date [{}] Requesting retrieval", DateUtils.truncate(endDate.toDate(), Calendar.DATE));
                Date midnight = endDate.toDateMidnight().toDate();
                if(!missingDatas.contains(midnight)) {
                    missingDatas.add(new ImmutablePair<>(company.getExchange(), midnight));
                }
                return;
            }

            Direction quoteDirection = Direction.None;

            if(quoteAtPrediction.getOpen() > quoteAtEndOfPrediction.getClose())
                quoteDirection = Direction.Down;

            if(quoteAtPrediction.getOpen() < quoteAtEndOfPrediction.getClose())
                quoteDirection = Direction.Up;

            if(quoteAtPrediction.getOpen() == quoteAtEndOfPrediction.getClose())
                quoteDirection = Direction.None;

            prediction.setCorrect(quoteDirection == prediction.getDirection());
            prediction.setActualChange(quoteAtEndOfPrediction.getClose() - quoteAtPrediction.getOpen());

            double actualEarningPerShare = Math.abs(prediction.getLastBid() - (prediction.getLastAsk() - prediction.getActualChange()));

            prediction.setActualEarningPerShare(actualEarningPerShare);

            LOG.info("Prediction Validated for Company [{}] Direction [{}] - Correct? [{}]", company.getName(), prediction.getDirection(), prediction.getCorrect());

            predictionRepository.save(prediction);

        });

//      Load data for missing dates
        missingDatas.forEach(date -> {
            LOG.debug("Requesting Retrieval of Quote data for Date [{}] for Exchange [{}]", DateUtils.truncate(date.getValue(), Calendar.DATE), date.getKey());
            missingQuoteData(date);
        });

    }

    private Quote getQuoteAtDate(final Exchange exchange, final Company company, final Date date) {
        if(exchange.isIntraday()) {
            List<Quote> quotes = quoteRepository.findByCompanyAndIntradayAndDateLessThan(company.getId(), false, date, new PageRequest(0,1, new Sort(Sort.Direction.DESC, "date")));

            if(!quotes.isEmpty()) {
                return quotes.get(0);
            }
        }

        return quoteRepository.findByCompanyAndDateAndIntraday(company.getId(), DateUtils.truncate(date, Calendar.DATE), false);
    }

    public static Predicate<Prediction> isOverdue() {
        return prediction -> new DateTime(prediction.getPredictionDate()).plusMillis(prediction.getValidityPeriod().intValue()).isBeforeNow();
    }

}
