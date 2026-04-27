using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Application.Interfaces.Scheduling;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Scheduling.Update;

public class UpdateScheduleHandler : IRequestHandler<UpdateScheduleCommand, Result<UpdateScheduleResponse>>
{
    private readonly ISchedulerService _schedulerService;
    private readonly ILogger<UpdateScheduleHandler> _logger;

    public UpdateScheduleHandler(ISchedulerService schedulerService, ILogger<UpdateScheduleHandler> logger)
    {
        _schedulerService = schedulerService;
        _logger = logger;
    }

    public async Task<Result<UpdateScheduleResponse>> Handle(UpdateScheduleCommand request, CancellationToken cancellationToken)
    {
        var schedule = await _schedulerService.GetScheduleByIdAsync(request.ScheduleId);

        if (schedule == null)
        {
            return Result<UpdateScheduleResponse>.Error("Schedule not found");
        }

        // Update fields if provided
        if (!string.IsNullOrEmpty(request.Name)) schedule.Name = request.Name;
        if (request.Description != null) schedule.Description = request.Description;
        if (request.ScheduleType.HasValue) schedule.ScheduleType = request.ScheduleType.Value;
        if (request.CronExpression != null) schedule.CronExpression = request.CronExpression;
        if (request.RecurrenceInterval.HasValue) schedule.RecurrenceInterval = request.RecurrenceInterval;
        if (request.StartTime.HasValue) schedule.StartTime = request.StartTime;
        if (!string.IsNullOrEmpty(request.TimeZone)) schedule.TimeZone = request.TimeZone;
        if (request.StepsToExecute != null) schedule.StepsToExecute = request.StepsToExecute;
        if (request.StepConfigurations != null) schedule.StepConfigurations = request.StepConfigurations;
        if (request.ExportCleanedData.HasValue) schedule.ExportCleanedData = request.ExportCleanedData.Value;
        if (request.ExportMatchedData.HasValue) schedule.ExportMatchedData = request.ExportMatchedData.Value;
        if (request.ExportFormattedData.HasValue) schedule.ExportFormattedData = request.ExportFormattedData.Value;
        if (request.IsEnabled.HasValue) schedule.IsEnabled = request.IsEnabled.Value;
        schedule.ConnectionInfo = request.ConnectionInfo;

        var updated = await _schedulerService.UpdateScheduleAsync(schedule);

        return Result<UpdateScheduleResponse>.Success(new UpdateScheduleResponse(updated.Id, updated.NextRun));
    }
}
