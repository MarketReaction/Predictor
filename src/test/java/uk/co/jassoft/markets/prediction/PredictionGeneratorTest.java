package uk.co.jassoft.markets.prediction;

import uk.co.jassoft.markets.datamodel.Direction;
import uk.co.jassoft.markets.datamodel.company.CompanyBuilder;
import uk.co.jassoft.markets.datamodel.company.quote.QuoteBuilder;
import uk.co.jassoft.markets.datamodel.company.sentiment.EntitySentiment;
import uk.co.jassoft.markets.datamodel.company.sentiment.StorySentimentBuilder;
import uk.co.jassoft.markets.datamodel.learningmodel.LearningModelRecordBuilder;
import uk.co.jassoft.markets.datamodel.prediction.Prediction;
import uk.co.jassoft.markets.datamodel.prediction.PredictionBuilder;
import uk.co.jassoft.markets.repository.*;
import uk.co.jassoft.utils.BaseRepositoryTest;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Created by jonshaw on 16/03/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SpringConfiguration.class)
@IntegrationTest
public class PredictionGeneratorTest extends BaseRepositoryTest {

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private QuoteRepository quoteRepository;

    @Autowired
    private LearningModelRepository learningModelRepository;

    @Autowired
    private PredictionRepository predictionRepository;

    @Autowired
    private StorySentimentRepository storySentimentRepository;

    @Autowired
    private PredictionGenerator target;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        predictionRepository.deleteAll();
        companyRepository.deleteAll();
        quoteRepository.deleteAll();
        learningModelRepository.deleteAll();
    }

    @Test
    public void testOnMessage_withNoQuotes_doesNotGeneratePrediction() throws Exception {
        String companyId = companyRepository.save(CompanyBuilder.aCompany()
                .build())
                .getId();

        target.generatePrediction(companyId);

        assertEquals(0, predictionRepository.count());
    }

    @Test
    public void testOnMessage_withNotEnoughQuotes_doesNotGeneratePrediction() throws Exception {
        String companyId = companyRepository.save(CompanyBuilder.aCompany()
                .build())
                .getId();

        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime(2016, 3, 1, 0, 0, 0).toDate())
                .withClose(100)
                .build());
        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime(2016, 3, 2, 0, 0, 0).toDate())
                .withClose(98)
                .build());
        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime(2016, 3, 3, 0, 0, 0).toDate())
                .withClose(96)
                .build());

        target.generatePrediction(companyId);

        assertEquals(0, predictionRepository.count());
    }


    @Test
    public void testOnMessage_withLearningModelDataAndNoSentiments_doesNotGeneratePrediction() throws Exception {
        String companyId = companyRepository.save(CompanyBuilder.aCompany()
                .build())
                .getId();

        storySentimentRepository.save(StorySentimentBuilder.aStorySentiment()
                .withCompany(companyId)
                .withStoryDate((new DateTime().minusDays(5).toDate()))
                .withEntitySentiment(Arrays.asList(new EntitySentiment("TestEntity", -2)))
                .build());

        stageValidQuotes(companyId);

        target.generatePrediction(companyId);

        assertEquals(0, predictionRepository.count());
    }

    @Test
    public void testOnMessage_withLearningModelDataAndNotEnoughSentiments_doesNotGeneratePrediction() throws Exception {
        String companyId = companyRepository.save(CompanyBuilder.aCompany()
                .build())
                .getId();

        storySentimentRepository.save(StorySentimentBuilder.aStorySentiment()
                .withCompany(companyId)
                .withStoryDate((new DateTime().minusDays(5).toDate()))
                .withEntitySentiment(Arrays.asList(new EntitySentiment("TestEntity", -2)))
                .build());

        stageValidQuotes(companyId);

        target.generatePrediction(companyId);

        assertEquals(0, predictionRepository.count());
    }

    @Test
    public void testOnMessage_withLearningModelDataAndNoPreviousPredictions_generatesPredictionWithGuessCertainty() throws Exception {
        String companyId = generateCompanyForPrediction();

        stageValidQuotes(companyId);

        learningModelRepository.save(LearningModelRecordBuilder.aLearningModelRecord()
                .withCompany(companyId)
                .withPreviousQuoteDirection(Direction.Down)
                .withPreviousSentimentDirection(Direction.Down)
                .withLastSentimentDifferenceFromAverage(-5)
                .withResultingQuoteChange(-2)
                .build());

        target.generatePrediction(companyId);

        assertEquals(1, predictionRepository.count());
        assertEquals("0.5", predictionRepository.findAll().get(0).getCertainty().toString());
        assertEquals("-2.0", predictionRepository.findAll().get(0).getPredictedChange().toString());
    }

    @Test
    public void testOnMessage_withLearningModelDataAndPreviousPredictions_generatesPredictionWithDefinateCertainty() throws Exception {
        String companyId = generateCompanyForPrediction();

        stageValidQuotes(companyId);

        learningModelRepository.save(LearningModelRecordBuilder.aLearningModelRecord()
                .withCompany(companyId)
                .withPreviousQuoteDirection(Direction.Down)
                .withPreviousSentimentDirection(Direction.Down)
                .withLastSentimentDifferenceFromAverage(-5)
                .withResultingQuoteChange(-2)
                .build());

        generateCorrectPrediction(companyId);
        generateCorrectPrediction(companyId);
        generateCorrectPrediction(companyId);
        generateCorrectPrediction(companyId);

        target.generatePrediction(companyId);

        assertEquals(5, predictionRepository.count());
        assertEquals("1.0", predictionRepository.findAll().get(4).getCertainty().toString());
        assertEquals("-2.0", predictionRepository.findAll().get(4).getPredictedChange().toString());
    }

    @Test
    public void testOnMessage_withExistingPredictions_doesNotGenerateDuplicatePrediction() throws Exception {
        String companyId = generateCompanyForPrediction();

        stageValidQuotes(companyId);

        learningModelRepository.save(LearningModelRecordBuilder.aLearningModelRecord()
                .withCompany(companyId)
                .withPreviousQuoteDirection(Direction.Down)
                .withPreviousSentimentDirection(Direction.Down)
                .withLastSentimentDifferenceFromAverage(-5)
                .withResultingQuoteChange(-2)
                .build());


        generateCorrectPrediction(companyId);
        generateCorrectPrediction(companyId);
        generateCorrectPrediction(companyId);
        generateCorrectPrediction(companyId);

        target.generatePrediction(companyId);

        target.generatePrediction(companyId);

        assertEquals(5, predictionRepository.count());
        assertEquals("1.0", predictionRepository.findAll().get(4).getCertainty().toString());
        assertEquals("-2.0", predictionRepository.findAll().get(4).getPredictedChange().toString());
    }

    @Test
    public void testOnMessage_withFewExistingPredictions_generatesPredictionWithLowerCertainty() throws Exception {
        String companyId = generateCompanyForPrediction();

        stageValidQuotes(companyId);

        learningModelRepository.save(LearningModelRecordBuilder.aLearningModelRecord()
                .withCompany(companyId)
                .withPreviousQuoteDirection(Direction.Down)
                .withPreviousSentimentDirection(Direction.Down)
                .withLastSentimentDifferenceFromAverage(-5)
                .withResultingQuoteChange(-2)
                .build());

        generateCorrectPrediction(companyId);

        target.generatePrediction(companyId);

        target.generatePrediction(companyId);

        assertEquals(2, predictionRepository.count());
        assertEquals("0.6", predictionRepository.findAll().get(1).getCertainty().toString());
        assertEquals("-2.0", predictionRepository.findAll().get(1).getPredictedChange().toString());
    }

    private void generateCorrectPrediction(String companyId) {
        Prediction prediction = PredictionBuilder.aPrediction()
                .withCompany(companyId)
                .withPredictionDate(new DateTime().minusDays(2).toDate())
                .withDirection(Direction.Down)
                .build();

        prediction.setCorrect(true);

        predictionRepository.save(prediction);
    }

    private String generateCompanyForPrediction() {
        String companyId = companyRepository.save(CompanyBuilder.aCompany()
                .build())
                .getId();

        storySentimentRepository.save(StorySentimentBuilder.aStorySentiment()
                        .withCompany(companyId)
                        .withStoryDate((new DateTime().minusDays(5).toDate()))
                        .withEntitySentiment(Arrays.asList(new EntitySentiment("TestEntity", -2)))
                        .build());

        storySentimentRepository.save(StorySentimentBuilder.aStorySentiment()
                        .withCompany(companyId)
                        .withStoryDate((new DateTime().minusDays(4).toDate()))
                        .withEntitySentiment(Arrays.asList(new EntitySentiment("TestEntity", -3)))
                        .build());

        storySentimentRepository.save(StorySentimentBuilder.aStorySentiment()
                        .withCompany(companyId)
                        .withStoryDate((new DateTime().minusDays(3).toDate()))
                        .withEntitySentiment(Arrays.asList(new EntitySentiment("TestEntity", -5)))
                        .build());

        storySentimentRepository.save(StorySentimentBuilder.aStorySentiment()
                        .withCompany(companyId)
                        .withStoryDate((new DateTime().minusDays(2).toDate()))
                        .withEntitySentiment(Arrays.asList(new EntitySentiment("TestEntity", -1)))
                        .build());

        storySentimentRepository.save(StorySentimentBuilder.aStorySentiment()
                        .withCompany(companyId)
                        .withStoryDate((new DateTime().minusDays(2).toDate()))
                        .withEntitySentiment(Arrays.asList(new EntitySentiment("TestEntity2", -2)))
                        .build());

        storySentimentRepository.save(StorySentimentBuilder.aStorySentiment()
                        .withCompany(companyId)
                        .withStoryDate((new DateTime().minusDays(1).toDate()))
                        .withEntitySentiment(Arrays.asList(new EntitySentiment("TestEntity", -8)))
                        .build());

        return companyId;

    }

    private void stageValidQuotes(final String companyId) {

        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime().minusDays(6).toDate())
                .withClose(100)
                .build());
        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime().minusDays(5).toDate())
                .withClose(98)
                .build());
        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime().minusDays(4).toDate())
                .withClose(96)
                .build());
        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime().minusDays(3).toDate())
                .withClose(94)
                .build());
        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime().minusDays(2).toDate())
                .withClose(92)
                .build());
        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime().minusDays(1).toDate())
                .withClose(90)
                .build());
        quoteRepository.save(QuoteBuilder.aQuote()
                .withCompany(companyId)
                .withDate(new DateTime().toDate())
                .withClose(88)
                .build());
    }
}