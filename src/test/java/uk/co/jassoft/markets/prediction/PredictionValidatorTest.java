package uk.co.jassoft.markets.prediction;

import uk.co.jassoft.markets.datamodel.Direction;
import uk.co.jassoft.markets.datamodel.company.CompanyBuilder;
import uk.co.jassoft.markets.datamodel.company.ExchangeBuilder;
import uk.co.jassoft.markets.datamodel.company.quote.QuoteBuilder;
import uk.co.jassoft.markets.datamodel.prediction.PredictionBuilder;
import uk.co.jassoft.markets.repository.CompanyRepository;
import uk.co.jassoft.markets.repository.ExchangeRepository;
import uk.co.jassoft.markets.repository.PredictionRepository;
import uk.co.jassoft.markets.repository.QuoteRepository;
import uk.co.jassoft.utils.BaseRepositoryTest;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by jonshaw on 17/03/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SpringConfiguration.class)
@IntegrationTest
public class PredictionValidatorTest extends BaseRepositoryTest {

    @Autowired
    private PredictionRepository predictionRepository;

    @Autowired
    private QuoteRepository quoteRepository;

    @Autowired
    private ExchangeRepository exchangeRepository;

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private PredictionValidator target;

    private String companyId;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        predictionRepository.deleteAll();
        quoteRepository.deleteAll();
        companyRepository.deleteAll();

        String exchnageId = exchangeRepository.save(ExchangeBuilder.anExchange().build()).getId();

        companyId = companyRepository.save(CompanyBuilder.aCompany()
                .withExchange(exchnageId)
                .build())
                .getId();
    }


    @Test
    public void testValidatePredictions_withNoStartQuoteData_doesNotValidate() throws Exception {

        predictionRepository.save(PredictionBuilder.aPrediction()
                .withCompany(companyId)
                .withPredictionDate(new DateTime(2016, 3, 1, 0, 0, 0).toDate())
                .withValidityPeriod(100000l)
                .withDirection(Direction.Down)
                .withLastBid(100)
                .withLastAsk(102)
                .build());

        target.validatePredictions();

        assertEquals(1, predictionRepository.count());
        assertNull(predictionRepository.findAll().get(0).getCorrect());

    }

    @Test
    public void testValidatePredictions_withNoEndQuoteData_doesNotValidate() throws Exception {

        predictionRepository.save(PredictionBuilder.aPrediction()
                .withCompany(companyId)
                .withPredictionDate(new DateTime(2016, 3, 1, 0, 0, 0).toDate())
                .withValidityPeriod(86400000l)
                .withDirection(Direction.Down)
                .withLastBid(100)
                .withLastAsk(102)
                .build());

        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime(2016, 3, 1, 0, 0, 0).toDate())
                .withOpen(100)
                .build());

        target.validatePredictions();

        assertEquals(1, predictionRepository.count());
        assertNull(predictionRepository.findAll().get(0).getCorrect());

    }

    @Test
    public void testValidatePredictions_withValidData_ValidatesCorrectly() throws Exception {

        predictionRepository.save(PredictionBuilder.aPrediction()
                .withCompany(companyId)
                .withPredictionDate(new DateTime(2016, 3, 1, 0, 0, 0).toDate())
                .withValidityPeriod(86400000l)
                .withDirection(Direction.Down)
                .withLastBid(100)
                .withLastAsk(102)
                .build());

        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime(2016, 3, 1, 0, 0, 0).toDate())
                .withOpen(100)
                .build());

        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime(2016, 3, 2, 0, 0, 0).toDate())
                .withClose(98)
                .build());

        target.validatePredictions();

        assertEquals(1, predictionRepository.count());
        assertEquals(true, predictionRepository.findAll().get(0).getCorrect());
        assertEquals("-2.0", predictionRepository.findAll().get(0).getActualChange().toString());
        assertEquals("4.0", predictionRepository.findAll().get(0).getActualEarningPerShare().toString());

    }
}