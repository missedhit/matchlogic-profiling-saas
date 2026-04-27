using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Features.DataMatching.FellegiSunter;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Phonetics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MatchLogic.Domain.Entities;
using MatchLogic.Infrastructure.Comparator;
using MatchLogic.Infrastructure.Phonetics;
using Dasync.Collections;
using MatchLogic.Application.Features.Import;
using MatchLogic.Infrastructure.Persistence;
using Microsoft.Extensions.Logging;
using Moq;
using MatchLogic.Application.Interfaces.Events;
using Xunit;
using Microsoft.Extensions.Options;
using MatchLogic.Domain.Import;

namespace MatchLogic.Application.UnitTests;
public class OptimizedEMTests
{
    private readonly IStringSimilarityCalculator _similarityCalculator;
    private readonly ITransliterator _transliterator;
    private readonly IPhoneticEncoder _phoneticEncoder;
    private readonly PhoneticConverter _phoneticConverter;
    private IComparator _comparator;
    private readonly ComparatorBuilder _builder;
    private readonly Mock<ILogger<OptimizedEMTests>> _logger;
    private readonly Mock<IStepProgressTracker> _mockProgressTracker;
    private readonly ProbabilisticOption probabilisticOption = new ProbabilisticOption() { MaxDegreeOfParallelism = 2, DecimalPlaces = 3 };
    public OptimizedEMTests()
    {
        _logger = new Mock<ILogger<OptimizedEMTests>>();
        _mockProgressTracker = new Mock<IStepProgressTracker>();
        var args = new Dictionary<ArgsValue, string>
        {
            {ArgsValue.Level, "0.0" }
        };
        // Use actual JaroWinkler Calculator
        _similarityCalculator = new JaroWinklerCalculator();

        //Use actual Unidecode Transliterator and Phonix Encoder
        _transliterator = new UnidecodeTransliterator();
        _phoneticEncoder = new PhonixEncoder();

        //Use actual PhoneticConverter
        _phoneticConverter = new PhoneticConverter(_transliterator, _phoneticEncoder);

        // Create factories
        var configFactory = new ComparatorConfigFactory();
        var strategyFactory = new ComparatorStrategyFactory(_similarityCalculator, _phoneticConverter);

        // Create builder
        _builder = new ComparatorBuilder(configFactory, strategyFactory);
        _comparator = _builder.WithArgs(args).Build();

    }
    private IAsyncEnumerable<Dictionary<string, object>> CreateTestRecords()
    {
        return new List<Dictionary<string, object>>
        {
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Smith" } },
            new Dictionary<string, object> { { "FirstName", "Jon" }, { "LastName", "Smith" } },
            new Dictionary<string, object> { { "FirstName", "Jane" }, { "LastName", "Doe" } },
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Doe" } }
        }.ToAsyncEnumerable();
    }

    private List<ProbabilisticMatchCriteria> CreateTestFields()
    {
        var args = new Dictionary<ArgsValue, string>
        {
            {ArgsValue.Level, "0.0" }
        };
        return new List<ProbabilisticMatchCriteria>
        {
            new ProbabilisticMatchCriteria("FirstName", _builder,args, probabilisticOption),
            new ProbabilisticMatchCriteria("LastName", _builder,args, probabilisticOption)
        };
    }

    [Fact]
    public void Initialize_WithValidData_ShouldSetupCorrectProbabilities()
    {
        // Arrange
        var records = CreateTestRecords();
        var fields = CreateTestFields();
        var em = new OptimizedEM(Options.Create(probabilisticOption));

        // Act
        em.Initialize(fields, records);

        // Assert
        var results = em.GetResults().ToList();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.True(r.MProb >= 0 && r.MProb <= 1));
        Assert.All(results, r => Assert.True(r.UProb >= 0 && r.UProb <= 1));
    }

    [Fact]
    public void RunEM_ShouldConvergeWithValidResults()
    {
        // Arrange
        var records = CreateTestRecords();
        var fields = CreateTestFields();
        var em = new OptimizedEM(Options.Create(probabilisticOption));

        // Act
        em.Initialize(fields, records);
        em.RunEM();

        // Assert
        var results = em.GetResults().ToList();
        var patterns = em.GetPatterns().ToList();

        Assert.NotEmpty(patterns);
        Assert.All(results, r => Assert.True(r.MProb > r.UProb));
    }

    [Fact]
    public void Initialize_WithIdenticalRecords_ShouldAggregatePatterns()
    {
        // Arrange
        var records = new List<Dictionary<string, object>>
        {
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Smith" } },
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Smith" } },
            new Dictionary<string, object> { { "FirstName", "Jane" }, { "LastName", "Doe" } }
        }.ToAsyncEnumerable();
        var fields = CreateTestFields();
        var em = new OptimizedEM(Options.Create(probabilisticOption));

        // Act
        em.Initialize(fields, records);

        // Assert
        var patterns = em.GetPatterns().ToList();
        Assert.Contains(patterns, p => p.Count > 1);
    }

    //[Fact]
    //public void Initialize_WithEmptyRecords_ShouldThrowException()
    //{
    //    // Arrange
    //    var fields = CreateTestFields();
    //    var em = new OptimizedEM(fields);

    //    // Act & Assert
    //    Assert.Throws<ArgumentException>(() =>
    //        em.Initialize(new List<Dictionary<string, object>>()));
    //}

    //[Fact]
    //public void Initialize_WithSingleRecord_ShouldThrowException()
    //{
    //    // Arrange
    //    var records = new List<Dictionary<string, object>>
    //    {
    //        new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Smith" } }
    //    };
    //    var fields = CreateTestFields();
    //    var em = new OptimizedEM(fields);

    //    // Act & Assert
    //    Assert.Throws<ArgumentException>(() => em.Initialize(records));
    //}

    [Fact]
    public void RunEM_WithPerfectMatches_ShouldHaveHighMProbabilities()
    {
        // Arrange
        var records = new List<Dictionary<string, object>>
        {
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Smith" } },
            new Dictionary<string, object> { { "FirstName", "John" }, { "LastName", "Smith" } }
        }.ToAsyncEnumerable();
        var fields = CreateTestFields();
        var em = new OptimizedEM(Options.Create(probabilisticOption));

        // Act
        em.Initialize(fields, records);
        em.RunEM();

        // Assert
        var results = em.GetResults().ToList();
        Assert.All(results, r => Assert.True(r.MProb > 0.8));
    }

    [Fact]
    public void RunEM_WithMixedMatchTypes_ShouldProduceAppropriateWeights()
    {
        var args = new Dictionary<ArgsValue, string>
        {
            {ArgsValue.Level, "0.0" }
        };
        // Arrange
        var records = new List<Dictionary<string, object>>
    {
        // Record 1 - Base record
        new Dictionary<string, object> {
            { "FirstName", "John" },
            { "LastName", "Smith" },
            { "Address", "123 Main Street" },
            { "Phone", "555-0123" }
        },
        // Record 2 - Exact match with Record 1
        new Dictionary<string, object> {
            { "FirstName", "John" },
            { "LastName", "Smith" },
            { "Address", "123 Main Street" },
            { "Phone", "555-0123" }
        },
         //Record 3 - Partial matches with Record 1
        new Dictionary<string, object> {
            { "FirstName", "Jon" },  // Close to "John"
            { "LastName", "Smith" },  // Exact match
            { "Address", "123 Main St" },  // Similar but not exact
            { "Phone", "5550123" }  // Similar but formatted differently
        },
        // Record 4 - Some matches, some completely different from Record 1
        new Dictionary<string, object> {
            { "FirstName", "John" },  // Exact match
            { "LastName", "Williams" },  // Complete mismatch
            { "Address", "456 Oak Avenue" },  // Complete mismatch
            { "Phone", "555-0123" }  // Exact match
        },
        // Record 5 - Completely different from all others
        new Dictionary<string, object> {
            { "FirstName", "Alex" },
            { "LastName", "Doe" },
            { "Address", "Boulevard Road 123" },
            { "Phone", "132-0000" }
        }
    }.ToAsyncEnumerable();

        // Create comparison fields with appropriate comparators
        var fields = new List<ProbabilisticMatchCriteria>
    {
        new ProbabilisticMatchCriteria("FirstName", _builder, args, probabilisticOption),
        new ProbabilisticMatchCriteria("LastName", _builder, args, probabilisticOption),
        new ProbabilisticMatchCriteria("Address", _builder, args, probabilisticOption),
        new ProbabilisticMatchCriteria("Phone", _builder, args, probabilisticOption)
    };

        var em = new OptimizedEM(Options.Create(probabilisticOption));

        // Act
        em.Initialize(fields, records);
        em.RunEM();

        // Get results and patterns
        var results = em.GetResults().ToList();
        var patterns = em.GetPatterns().ToList();

        // Assert
        // 1. Check that we have results for all fields
        Assert.Equal(4, results.Count);

        // 2. Verify that all fields have valid probability ranges
        Assert.All(results, r =>
        {
            Assert.True(r.MProb >= 0 && r.MProb <= 1, $"M probability for {r.FieldName} should be between 0 and 1");
            Assert.True(r.UProb >= 0 && r.UProb <= 1, $"U probability for {r.FieldName} should be between 0 and 1");
        });

        // 3. Verify that M probabilities are higher than U probabilities
        Assert.All(results, r => Assert.True(r.MProb > r.UProb,
            $"Field {r.FieldName} should have M probability higher than U probability"));

        // 4. Check that we have multiple patterns with different posteriors
        Assert.True(patterns.Count > 1, "Should have multiple comparison patterns");
        Assert.Contains(patterns, p => p.Posterior > 0.8); // High confidence matches
        // Assert.Contains(patterns, p => p.Posterior < 0.2); // Low confidence matches

        // 5. Verify at least one high-probability exact match pattern exists
        var exactMatch = patterns.FirstOrDefault(p => p.Posterior > 0.9);
        Assert.NotNull(exactMatch);

        // 6. Verify we have some partial match patterns
        var partialMatches = patterns.Where(p => p.Posterior >= 5E-06 && p.Posterior < 0.9);
        Assert.True(partialMatches.Any());
    }  

    [Fact]
    public void FieldStatistics_Calculate_ShouldReturnCorrectValues()
    {
        // Arrange
        var values = new[] { "John", "John", "Jane" };

        var termIndex = new TermFrequencyIndex();
        termIndex.IndexTerms(new List<IDictionary<string, object>> {
        new Dictionary<string, object> { { "Test", "John" } },
        new Dictionary<string, object> { { "Test", "John" } },
        new Dictionary<string, object> { { "Test", "Jane" } }
    });
        // Act
        var stats = FieldStatistics.Calculate(values, termIndex, "Test");

        // Assert
        Assert.True(stats.Entropy > 0);
        Assert.Equal(2.0 / 3.0, stats.UniqueRatio);
    }

    [Fact]
    public void SparseVector_Operations_ShouldWorkCorrectly()
    {
        // Arrange
        var vector = new SparseVector();

        // Act
        vector.Add(0, 1.0);
        vector.Add(1, 0.0);  // Should not be stored
        vector.Add(2, 0.5);

        // Assert
        Assert.Equal(1.0, vector.Get(0));
        Assert.Equal(0.0, vector.Get(1));
        Assert.Equal(0.5, vector.Get(2));
    }

    [Fact]
    public void SimilarityMatrix_Operations_ShouldWorkCorrectly()
    {
        // Arrange
        var matrix = new SimilarityMatrix();

        // Act
        matrix.AddSimilarity("FirstName", 0, 1.0);
        matrix.AddSimilarity("FirstName", 1, 0.0);
        matrix.AddSimilarity("LastName", 0, 0.5);

        // Assert
        var firstNameSims = matrix.GetFieldSimilarities("FirstName").ToList();
        Assert.Single(firstNameSims);
        Assert.Equal(1.0, firstNameSims[0].Value);
    }

    [Theory]
    [InlineData(1.0, 1.0, true)]  // Maximum values
    [InlineData(0.5, 0.5, true)]  // Mid-range values
    [InlineData(-0.1, 0.0, false)] // Invalid negative value
    public void ProbabilityValues_ShouldBeInValidRange(double mProb, double uProb, bool isValid)
    {
        var args = new Dictionary<ArgsValue, string>
    {
        {ArgsValue.Level, "0.0" }
    };
        var comparatorname = _builder.WithArgs(args).Build();

        // Arrange
        var field = new ProbabilisticMatchCriteria("Test", _builder, args, probabilisticOption);

        // Act & Assert
        if (isValid)
        {
            // Test each comparison level
            foreach (var level in field.Settings.Levels)
            {
                level.M_Probability = mProb;
                level.U_Probability = uProb;
                level.UpdateWeight();

                Assert.Equal(mProb, level.M_Probability);
                Assert.Equal(uProb, level.U_Probability);
            }
        }
        else
        {
            //// For invalid values, we should test that they're not accepted
            //Assert.Throws<ArgumentException>(() =>
            //{
            //    var level = field.Settings.Levels.First();
            //    if (mProb < 0 || mProb > 1)
            //    {
            //        level.MProbability = mProb;
            //    }
            //    if (uProb < 0 || uProb > 1)
            //    {
            //        level.UProbability = uProb;
            //    }
            //});
        }
    }

    //[Fact]
    public async void Expectation_Maxmimisation_End_to_End()
    {
        var _excelFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "Example Data 1.xlsx");
        var _dbFilePath = Path.GetTempFileName();
        var excelReader = new Infrastructure.Import.ExcelDataReader(new ExcelConnectionConfig() { FilePath = _excelFilePath }, _logger.Object);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger.Object);
        var importModule = new DataImportModule(excelReader, liteDbStore, _logger.Object);

        var jobId = await importModule.ImportDataAsync(cancellationToken: CancellationToken.None);

        var data = liteDbStore.StreamJobDataAsync(jobId, _mockProgressTracker.Object);

        var em = new OptimizedEM(Options.Create(probabilisticOption));

        var args = new Dictionary<ArgsValue, string>
        {
            {ArgsValue.Level, "0.0" }
        };

        //_builder.WithArgs(args).Build();
        var fields = new List<ProbabilisticMatchCriteria>
    {
        new ProbabilisticMatchCriteria("City", _builder,args, probabilisticOption),
        new ProbabilisticMatchCriteria("Company Name", _builder,args , probabilisticOption),
    };

        // Act
        em.Initialize(fields, data);

        em.RunEM();

        //excelReader.Dispose();
    }
}