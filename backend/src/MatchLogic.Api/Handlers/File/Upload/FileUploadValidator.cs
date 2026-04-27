using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using Microsoft.AspNetCore.Http;
using System;
using System.IO;
using System.Linq;
using MatchLogic.Api.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Api.Common.Validators;

namespace MatchLogic.Api.Handlers.File.Upload;

public class FileUploadValidator : AbstractValidator<FileUploadRequest>
{
    private const long MaxFileSize = 5 * 1024 * 1024; // 5 MB


    public FileUploadValidator(IGenericRepository<Domain.Project.Project, Guid> projectRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;
        RuleFor(x => x.File)
            .NotNull().WithMessage("No file was uploaded.")
            .Must(file => file?.Length > 0).WithMessage(ValidationMessages.IsEmpty("File"))
            .Must(HaveValidExtension).WithMessage($"File extension is not allowed. Allowed extensions are: {string.Join(", ", ApiConstants.AllowedExtensions)}")
            //.Must(file => file.Length <= MaxFileSize).WithMessage($"File size must be less than {MaxFileSize / (1024 * 1024)} MB.")
            .Must(HaveValidMimeType).WithMessage(ValidationMessages.Invalid("Mime type"));


        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));


        RuleFor(x => x.SourceType)
            .Must(x => Enum.TryParse(x, true, out DataSourceType result))
            .WithMessage(ValidationMessages.Invalid("SourceType"))
            .WithErrorCode(ErrorCodeConstants.NotExists);

    }
    private bool HaveValidExtension(IFormFile file)
    {
        if (file == null) return false;
        string ext = Path.GetExtension(file.FileName).ToLowerInvariant();
        return ApiConstants.AllowedExtensions.Contains(ext);
    }

    private bool HaveValidMimeType(IFormFile file)
    {
        if (file == null) return false;

        return ApiConstants.AllowedMimeTypes.Contains(file.ContentType);
    }
}
