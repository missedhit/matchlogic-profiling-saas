using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Domain.Scheduling;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Scheduling.Create;

public class CreateScheduleHandler : IRequestHandler<CreateScheduleCommand, Result<CreateScheduleResponse>>
{
    private readonly ISchedulerService _schedulerService;
    private readonly ILogger<CreateScheduleHandler> _logger;

    public CreateScheduleHandler(
        ISchedulerService schedulerService,
        ILogger<CreateScheduleHandler> logger)
    {
        _schedulerService = schedulerService;
        _logger = logger;
    }

    public async Task<Result<CreateScheduleResponse>> Handle(
        CreateScheduleCommand request,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Creating schedule: {Name} for project {ProjectId}",
            request.Name, request.ProjectId);

        // Map command to entity
        var schedule = new ScheduledTask
        {
            ProjectId = request.ProjectId,
            Name = request.Name,
            Description = request.Description,

            ScheduleType = request.ScheduleType,
            CronExpression = request.CronExpression,
            RecurrenceInterval = request.RecurrenceInterval,
            StartTime = request.StartTime,
            TimeZone = request.TimeZone,

            StepsToExecute = request.StepsToExecute,
            StepConfigurations = request.StepConfigurations,

            ExportCleanedData = request.ExportCleanedData,
            ExportMatchedData = request.ExportMatchedData,
            ExportFormattedData = request.ExportFormattedData,
            ExportDataProfile = request.ExportDataProfile,
            ExportSummaryReport = request.ExportSummaryReport,

            NotifyOnSuccess = request.NotifyOnSuccess,
            NotifyOnFailure = request.NotifyOnFailure,
            NotificationEmails = request.NotificationEmails,

            EnableRetry = request.EnableRetry,
            MaxRetryAttempts = request.MaxRetryAttempts,

            ConnectionInfo = request.ConnectionInfo,

            Tags = request.Tags
        };

        // Create schedule (registers with Hangfire internally)
        var createdSchedule = await _schedulerService.CreateScheduleAsync(schedule);

        _logger.LogInformation("Schedule created: {ScheduleId}, NextRun: {NextRun}",
            createdSchedule.Id, createdSchedule.NextRun);

        return Result<CreateScheduleResponse>.Success(
            new CreateScheduleResponse(createdSchedule.Id, createdSchedule.NextRun));
    }
}
