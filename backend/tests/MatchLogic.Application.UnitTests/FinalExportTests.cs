using MatchLogic.Application.Features.FinalExport;
using MatchLogic.Infrastructure.Persistence;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static MatchLogic.Application.Features.FinalExport.FinalScoresService;

namespace MatchLogic.Application.UnitTests
{
    public class FinalExportTests
    {
        private readonly string _dbFilePath;
        private readonly ILogger _logger;
        public FinalExportTests()
        {
            //_dbFilePath = Path.GetTempFileName();
            _dbFilePath = "C:\\ProgramData\\MatchLogicApi\\Database\\MatchLogic.db";
            using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            _logger = loggerFactory.CreateLogger<LiteDbDataStoreTest>();

        }

        [Fact]
        public async Task Test_ExportFunctionalityAsync()
        {
            // Arrange
            var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
            //var finalScoresService = new FinalScoresService(liteDbStore);
            var exportOptions = new FinalScoresFilter
            {
                ProjectId = Guid.Parse("60a80930-17df-4036-b0e9-921232bb5104"),
                //DataSourceGuids =
                //[
                //    Guid.Parse("4c976383-a7cd-464d-a9f5-18cd49ae405d"),
                //    Guid.Parse("27d4669c-3c7f-4e7f-af39-d1ef85056e80")
                //],
                Selected = false,
                //ExportMode = DuplicateExportMode.MasterOnly
            };
            // Act
            //var data = await finalScoresService.GetTransformedFinalScoresAsync(exportOptions,1,100);
            // (Execute the export command/module)
            // Assert
            // (Verify that the data was exported correctly)
        }
    }
}
