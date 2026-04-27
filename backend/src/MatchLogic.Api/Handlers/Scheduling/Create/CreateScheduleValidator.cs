using MatchLogic.Api.Common.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Domain.Scheduling;
using MatchLogic.Infrastructure.Scheduling;
using FluentValidation;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Scheduling.Create;

public class CreateScheduleValidator : AbstractValidator<CreateScheduleCommand>
{
    private readonly ISchedulerService _schedulerService;
    private readonly IGenericRepository<Domain.Project.Project, Guid> _projectRepository;

    public CreateScheduleValidator(ISchedulerService schedulerService, IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        _schedulerService = schedulerService;
        _projectRepository = projectRepository;

        RuleFor(x => x.Name)
            .NotEmpty().WithMessage("Schedule name is required")
            .MaximumLength(200).WithMessage("Schedule name cannot exceed 200 characters");

        RuleFor(x => x.ProjectId)
          .SetValidator(new ProjectIdValidator(projectRepository));

        // Cron expression validation
        When(x => x.ScheduleType == ScheduleType.Cron, () =>
        {
            RuleFor(x => x.CronExpression)
                .NotEmpty().WithMessage("Cron expression is required for Cron schedule type")
                .MustAsync(BeValidCronExpression).WithMessage("Invalid cron expression format");
        });

        // Simple schedule validation
        When(x => x.ScheduleType == ScheduleType.Simple, () =>
        {
            RuleFor(x => x.RecurrenceInterval)
                .NotNull().WithMessage("Recurrence interval is required for Simple schedule type")
                .Must(interval => interval.HasValue && interval.Value.TotalMinutes >= 1)
                .WithMessage("Recurrence interval must be at least 1 minute");

            RuleFor(x => x.StartTime)
                .NotNull().WithMessage("Start time is required for Simple schedule type");
        });

        // Steps validation
        RuleFor(x => x.StepsToExecute)
            .NotEmpty().WithMessage("At least one step must be configured");

        // TimeZone validation
        RuleFor(x => x.TimeZone)
            .Must(BeValidTimeZone).WithMessage("Invalid timezone identifier")
            .When(x => !string.IsNullOrEmpty(x.TimeZone));

        // Retry attempts validation
        RuleFor(x => x.MaxRetryAttempts)
            .InclusiveBetween(0, 10).WithMessage("Max retry attempts must be between 0 and 10");
    }

    private async Task<bool> ProjectExists(Guid projectId, CancellationToken cancellationToken)
    {
        var project = await _projectRepository.GetByIdAsync(projectId, "Projects");
        return project != null;
    }

    private async Task<bool> BeValidCronExpression(string cron, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(cron))
            return false;

        try
        {
            var result = await _schedulerService.ValidateCronExpressionAsync(cron);
            return result.IsValid;
        }
        catch
        {
            return false;
        }
    }

    private bool BeValidTimeZone(string timeZone)
    {
        try
        {
            TimeZoneInfo.FindSystemTimeZoneById(timeZone);
            return true;
        }
        catch
        {
            return false;
        }
    }
}