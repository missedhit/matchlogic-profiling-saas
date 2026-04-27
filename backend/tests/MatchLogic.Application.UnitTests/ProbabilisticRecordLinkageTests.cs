using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Features.DataMatching.FellegiSunter;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Phonetics;
using MatchLogic.Domain.Entities;
using MatchLogic.Infrastructure.Comparator;
using MatchLogic.Infrastructure.Phonetics;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MatchLogic.Application.Interfaces.Comparator;
using Microsoft.Extensions.Logging;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.Import;
using MatchLogic.Infrastructure.Persistence;
using Microsoft.Extensions.Options;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Infrastructure.BackgroundJob;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Common;

namespace MatchLogic.Application.UnitTests;
public class ProbabilisticRecordLinkageTests
{
    private readonly IStringSimilarityCalculator _similarityCalculator;
    private readonly ITransliterator _transliterator;
    private readonly IPhoneticEncoder _phoneticEncoder;
    private readonly PhoneticConverter _phoneticConverter;
    private IComparator _comparator;
    private readonly ComparatorBuilder _builder;
    private readonly Mock<ILogger<ProbabilisticRecordLinkageTests>> _logger;
    private readonly IExpectationMaximisation EM;
    private readonly IDataStore _mockDataStore;
    private readonly List<ProbabilisticMatchCriteria> _sampleFields;
    private readonly Guid _jobId;
    private readonly Mock<ITelemetry> _telemetryMock;
    private readonly SimpleRecordPairer _simpleRecordPairer;

    private readonly ProbabilisticOption probabilisticOption = new ProbabilisticOption() { MaxDegreeOfParallelism = 2, DecimalPlaces = 3 };

    public ProbabilisticRecordLinkageTests()
    {
        _jobId = Guid.NewGuid();
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

        _logger = new Mock<ILogger<ProbabilisticRecordLinkageTests>>();

        // Create builder
        _builder = new ComparatorBuilder(configFactory, strategyFactory);
        _comparator = _builder.WithArgs(args).Build();

        _sampleFields = new List<ProbabilisticMatchCriteria>
        {
            new ProbabilisticMatchCriteria("City", _builder,args, probabilisticOption) {MatchingType= MatchingType.Exact, Weight = 1.0},
        new ProbabilisticMatchCriteria("Company Name", _builder,args , probabilisticOption){ MatchingType = MatchingType.Fuzzy, Weight = 1.0 },
        };

        var _options = new RecordLinkageOptions();
        var optionsWrapper = new OptionsWrapper<RecordLinkageOptions>(_options);
         _simpleRecordPairer = new SimpleRecordPairer(new Mock<ILogger<SimpleRecordPairer>>().Object, optionsWrapper);
        //EM = new ParallelEM();
        _telemetryMock = new Mock<ITelemetry>();

        _telemetryMock.Setup(t => t.MeasureOperation(It.IsAny<string>()))
           .Returns(new Mock<IDisposable>().Object);
    }

    /*[Fact]
    public async Task Train_WithValidData_ShouldCompleteSuccessfully()
    {
        var _excelFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "Example Data 1.xlsx");
        var _dbFilePath = Path.GetTempFileName();
        var excelReader = new MatchLogic.Infrastructure.Import.ExcelDataReader(_excelFilePath, _logger.Object);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger.Object);
        var eventBus = new Mock<IEventBus>();
        eventBus.Setup(x=>x.PublishAsync<BaseEvent>(It.IsAny<BaseEvent>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        var jobEventPublisher = new JobEventPublisher(eventBus.Object);
        var importModule = new DataImportModule(excelReader, liteDbStore, _logger.Object);

        IBlockingStrategy blockingStrategy = new ExactMatchBlockingStrategy(It.IsAny<ILogger<ExactMatchBlockingStrategy>>());
        var jobId = await importModule.ImportDataAsync(CancellationToken.None);
        var logger = new Mock<ILogger<ParallelEM>>();
       
        // Arrange        
        var collectionName = GuidCollectionNameConverter.ToValidCollectionName(jobId);

        var EM = new ParallelEM(Constants.DefaultOptions.probabilisticOption);

        var linkage = new ProbabilisticRecordLinkage(EM, _sampleFields, liteDbStore,
            jobId, jobEventPublisher, blockingStrategy, _telemetryMock.Object, _simpleRecordPairer
            , logger.Object, Constants.DefaultOptions.probabilisticOption);

        // Act       
       await linkage.ExecuteAsync();
    }*/

//    public async Task Test_Linkage_Validation()
//    {
//        var _excelFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "Example Data 1.xlsx");
//        var _dbFilePath = Path.GetTempFileName();
//        var excelReader = new MatchLogic.Infrastructure.Import.ExcelDataReader(_excelFilePath, _logger.Object);
//        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger.Object);
//        var eventBus = new Mock<IEventBus>();
//        eventBus.Setup(x => x.PublishAsync<BaseEvent>(It.IsAny<BaseEvent>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
//        var jobEventPublisher = new JobEventPublisher(eventBus.Object);
//        var importModule = new DataImportModule(excelReader, liteDbStore, _logger.Object);

//        IBlockingStrategy blockingStrategy = new ExactMatchBlockingStrategy(It.IsAny<ILogger<ExactMatchBlockingStrategy>>());
//        var jobId = await importModule.ImportDataAsync(CancellationToken.None);
//        var logger = new Mock<ILogger<ParallelEM>>();

//        var criteria = new List<MatchCriteria>
//    {
//        new MatchCriteria { FieldName = "City",MatchingType= MatchingType.Exact, Weight = 1.0 },
//        new MatchCriteria { FieldName = "Company Name", MatchingType = MatchingType.Fuzzy, Weight = 1.0 },
//    };
//        // Arrange        
//        var collectionName = GuidCollectionNameConverter.ToValidCollectionName(jobId);

//        var EM = new ParallelEM();

//        var linkage = new ProbabilisticRecordLinkage(EM, _sampleFields, liteDbStore,
//            jobId, jobEventPublisher, blockingStrategy, _telemetryMock.Object, _simpleRecordPairer
//            , logger.Object);

//        var companyData = new List<Dictionary<string, object>>
//{
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Woods Landing Bar & Restaurant" },
//        { "Category", "Drinking places" },
//        { "Address", "9 State Highway 10" },
//        { "City", "ELM" },
//        { "State", "WY" },
//        { "Zip", "82063-9224" },
//        { "ContactName", "Sue E Spencer" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Medcom Group LTD" },
//        { "Category", "Physicians & Surgeons Equip & Supls-Whol" },
//        { "Address", "20579 County Road 103" },
//        { "City", "ELM" },
//        { "State", "WY" },
//        { "Zip", "82063-9230" },
//        { "ContactName", "Dixie J Green" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Printing & Graphics" },
//        { "Category", "Commercial printer" },
//        { "Address", "P.O. BOX 1466" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85211-1466" },
//        { "ContactName", "Tim W Dougherty" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Kingswood Collision Ctr" },
//        { "Category", "Automobile Body-Repairing & Painting" },
//        { "Address", "1015 W Broadway Rd" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85210-8473" },
//        { "ContactName", "Bill Baum" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Hairlights" },
//        { "Category", "Beauty Salons" },
//        { "Address", "1102 E University Dr # 2" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85203-8056" },
//        { "ContactName", "Judy A Markos" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Sarah's Hallmark" },
//        { "Category", "Gift Shops" },
//        { "Address", "1112 N Higley Rd # 103" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85205-6436" },
//        { "ContactName", "Jack J Babicke" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Printing Inc" },
//        { "Category", "Acoustical Materials" },
//        { "Address", "11520 E Marguerite Ave" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85208-5524" },
//        { "ContactName", "John A Daniels" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Shop The Knife Inc" },
//        { "Category", "Kitchenware" },
//        { "Address", "1445 W Southern Ave # 1208" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "ContactName", "Mike Beuzekom" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Michaels 9505" },
//        { "Category", "Retailer of toys and games" },
//        { "Address", "1505 S Power Rd" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85206-3707" },
//        { "ContactName", "Bill Wehrman" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Arizona Spirit Rv Park" },
//        { "Category", "Campgrounds" },
//        { "Address", "201 S Crismon Rd" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85208-4429" },
//        { "ContactName", "Gene S Despain" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "AM PM Mini Mart" },
//        { "Category", "Convenience Stores" },
//        { "Address", "2751 E University Dr" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "ContactName", "Andy S Patel" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Desert Haven Construction Inc" },
//        { "Category", "Single-family housing construction, nec" },
//        { "Address", "3022 N 80th St" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85207-9704" },
//        { "ContactName", "Neil J Lannuier" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Repair For Less" },
//        { "Category", "Automobile Repairing & Service" },
//        { "Address", "348 W 10th Ave # 1" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85210-3650" },
//        { "ContactName", "Eric Deroche" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Del Taco" },
//        { "Category", "Restaurants" },
//        { "Address", "3648 E Southern Ave" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85206-2504" },
//        { "ContactName", "Brad Klien" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Moriah Jewelers-Retail" },
//        { "Category", "Jewelers" },
//        { "Address", "4727 E Southern Ave" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "Zip", "85206-2757" },
//        { "ContactName", "Hwan A Kang" }
//    },
//    new Dictionary<string, object>
//    {
//        { "CompanyName", "Sun Villa Resort Renters" },
//        { "Category", "Apartment building operators" },
//        { "Address", "650 S 80th St W" },
//        { "City", "MESA" },
//        { "State", "Arizona" },
//        { "ContactName", "Gene Buckley" }
//    }
//};
//    }
}
