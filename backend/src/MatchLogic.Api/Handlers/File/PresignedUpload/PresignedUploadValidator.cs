using FluentValidation;
using MatchLogic.Api.Common;
using MatchLogic.Api.Common.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using System;
using System.IO;
using System.Linq;

namespace MatchLogic.Api.Handlers.File.PresignedUpload;

public class PresignedUploadValidator : AbstractValidator<PresignedUploadRequest>
{
    public PresignedUploadValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.FileName)
            .NotEmpty().WithMessage("FileName is required.")
            .Must(HaveValidExtension)
            .WithMessage($"File extension is not allowed. Allowed extensions are: {string.Join(", ", ApiConstants.AllowedExtensions)}");

        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));

        RuleFor(x => x.SourceType)
            .Must(x => Enum.TryParse(x, true, out DataSourceType _))
            .WithMessage(ValidationMessages.Invalid("SourceType"))
            .WithErrorCode(ErrorCodeConstants.NotExists);
    }

    private static bool HaveValidExtension(string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName)) return false;
        var ext = Path.GetExtension(fileName).ToLowerInvariant();
        return ApiConstants.AllowedExtensions.Contains(ext);
    }
}
