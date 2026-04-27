using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Project.DataProfiling;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Infrastructure.Project.Commands;
using MatchLogic.Infrastructure.Project.DataProfiling;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Project.Commands;

public class CommandFactory : ICommandFactory
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Dictionary<StepType, Type> _commands;

    public CommandFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
        _commands = new Dictionary<StepType, Type>
    {
        { StepType.Import, typeof(DataImportCommand) },
        { StepType.Profile, typeof(DataProfilingCommand) },
        { StepType.AdvanceProfile , typeof(AdvanceDataProfilingCommand) },
        { StepType.Cleanse, typeof(DataCleansingCommand) },
        { StepType.Match, typeof(MatchingCommand) },
         { StepType.Merge, typeof(MasterRecordDeterminationCommand) },
        { StepType.Overwrite, typeof(FieldOverwriteCommand) },
        { StepType.Export, typeof(FinalExportCommand) },
        // Add other handlers here
    };
    }

    public ICommand GetCommand(StepType stepType)
    {
        if (!_commands.TryGetValue(stepType, out var handlerType))
        {
            throw new InvalidOperationException($"No handler registered for step type {stepType}");
        }
        return (ICommand)_serviceProvider.GetRequiredService(handlerType);
    }
}
