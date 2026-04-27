using MatchLogic.Api.Handlers.Cleansing.Column;
using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Domain.CleansingAndStandaradization;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Api.Handlers.Cleansing;

public class CleansingMapper
{
    /// <summary>
    /// Maps a request to the appropriate command type
    /// </summary>
    public static TCommand MapToCommand<TCommand, TResponse>(BaseCleansingRule request)
        where TCommand : BaseCleansingRuleCommand<TResponse>, new()
        where TResponse : ICleansingRuleResponse
    {
        var command = new TCommand
        {
            ProjectId = request.ProjectId,
            DataSourceId = request.DataSourceId,
            isPreview = request.isPreview,
            StandardRules = new(),
            ExtendedRules = new(),
            MappingRules = new()
        };

        // Process each column operation
        foreach (var columnOp in request.ColumnOperations)
        {
            // Determine if we need extended rules (for copying)
            bool needsExtendedRule = columnOp.CopyField;

            // Process standard cleaning operations
            var cleaningOperations = columnOp.Operations
                .Where(op => op.Type == OperationType.Standard && op.CleaningType.HasValue)
                .ToList();

           
            foreach (var operation in cleaningOperations)
            {
                var rule = new CleaningRuleDto
                {
                    ColumnName = columnOp.ColumnName,
                    RuleType = operation.CleaningType.Value,
                    Arguments = operation.Parameters
                };
                command.StandardRules.Add(rule);
            }

            if (needsExtendedRule)
            {
                // Create extended rule with copying
                var extendedRule = new ExtendedCleaningRuleDto
                {
                    ColumnName = columnOp.ColumnName,
                    OperationType = OperationType.Standard,
                    ColumnMappings = new List<DataCleansingColumnMappingDto>
                            {
                                new DataCleansingColumnMappingDto
                                {
                                    SourceColumn = columnOp.ColumnName,
                                    TargetColumn = $"{columnOp.ColumnName}_Original"
                                }
                            }
                };
                command.ExtendedRules.Add(extendedRule);
            }

            // Process mapping operations
            var mappingOperations = columnOp.Operations
                .Where(op => op.Type == OperationType.Mapping && op.MappingType.HasValue)
                .ToList();

            foreach (var operation in mappingOperations)
            {
                var mappingRule = new MappingRuleDto
                {
                    OperationType = operation.MappingType.Value,
                    SourceColumn = operation.SourceColumns.Any()
                        ? operation.SourceColumns
                        : new List<string> { columnOp.ColumnName },
                    MappingConfig = operation.Parameters,
                    OutputColumns = operation.OutputColumns
                };
                command.MappingRules.Add(mappingRule);
            }
        }

        return command;
    }

    public static ColumnCleansingRuleCommand MapToCommand(BaseCleansingRule request)
    {
        var command = new ColumnCleansingRuleCommand
        {
            ProjectId = request.ProjectId,
            DataSourceId = request.DataSourceId,
            StandardRules = new(),
            ExtendedRules = new(),
            MappingRules = new()
        };


        // Process each column operation
        foreach (var columnOp in request.ColumnOperations)
        {
            // Determine if we need extended rules (for copying)
            bool needsExtendedRule = columnOp.CopyField;

            // Process standard cleaning operations
            var cleaningOperations = columnOp.Operations
                .Where(op => op.Type == OperationType.Standard && op.CleaningType.HasValue)
                .ToList();


            foreach (var operation in cleaningOperations)
            {
                var rule = new CleaningRuleDto
                {
                    ColumnName = columnOp.ColumnName,
                    RuleType = operation.CleaningType.Value,
                    Arguments = operation.Parameters
                };
                command.StandardRules.Add(rule);
            }

            if (needsExtendedRule)
            {
                // Create extended rule with copying
                var extendedRule = new ExtendedCleaningRuleDto
                {
                    ColumnName = columnOp.ColumnName,
                    OperationType = OperationType.Standard,
                    ColumnMappings = new List<DataCleansingColumnMappingDto>
                            {
                                new DataCleansingColumnMappingDto
                                {
                                    SourceColumn = columnOp.ColumnName,
                                    TargetColumn = $"{columnOp.ColumnName}_Original"
                                }
                            }
                };
                command.ExtendedRules.Add(extendedRule);
            }

            // Process mapping operations
            var mappingOperations = columnOp.Operations
                .Where(op => op.Type == OperationType.Mapping && op.MappingType.HasValue)
                .ToList();

            foreach (var operation in mappingOperations)
            {
                var mappingRule = new MappingRuleDto
                {
                    OperationType = operation.MappingType.Value,
                    SourceColumn = operation.SourceColumns.Any()
                        ? operation.SourceColumns
                        : new List<string> { columnOp.ColumnName },
                    MappingConfig = operation.Parameters,
                    OutputColumns = operation.OutputColumns
                };
                command.MappingRules.Add(mappingRule);
            }
        }

        return command;
    }
}
