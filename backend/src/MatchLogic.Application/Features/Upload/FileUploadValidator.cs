using FluentValidation;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Upload;
public class FileUploadValidator : AbstractValidator<FileUploadRequest>
{
    private readonly string[] AllowedExtensions = { ".csv", ".xlsx", ".xls" };
    public FileUploadValidator()
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;

        RuleFor(x => x.File)
            .NotNull().WithMessage("No file was uploaded.")
            .Must(file => file?.Length > 0).WithMessage("The file is empty.")
            .Must(HaveValidExtension).WithMessage($"File extension is not allowed. Allowed extensions are: {string.Join(", ", AllowedExtensions)}");
    }

    private bool HaveValidExtension(IFormFile file)
    {
        if (file == null) return false;
        var extension = Path.GetExtension(file.FileName).ToLowerInvariant();
        return AllowedExtensions.Contains(extension);
    }
}

