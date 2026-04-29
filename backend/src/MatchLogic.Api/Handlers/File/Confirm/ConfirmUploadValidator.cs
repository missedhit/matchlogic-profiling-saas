using FluentValidation;
using MatchLogic.Api.Common;
using MatchLogic.Api.Common.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using System;
using System.Linq;

namespace MatchLogic.Api.Handlers.File.Confirm;

public class ConfirmUploadValidator : AbstractValidator<ConfirmUploadRequest>
{
    public ConfirmUploadValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.FileId).NotEmpty();
        RuleFor(x => x.S3Key).NotEmpty();
        RuleFor(x => x.OriginalName).NotEmpty();

        RuleFor(x => x.FileExtension)
            .Must(ext => !string.IsNullOrWhiteSpace(ext)
                && ApiConstants.AllowedExtensions.Contains(ext.ToLowerInvariant()))
            .WithMessage($"File extension is not allowed. Allowed extensions are: {string.Join(", ", ApiConstants.AllowedExtensions)}");

        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));

        RuleFor(x => x.SourceType)
            .Must(x => Enum.TryParse(x, true, out DataSourceType _))
            .WithMessage(ValidationMessages.Invalid("SourceType"))
            .WithErrorCode(ErrorCodeConstants.NotExists);
    }
}
