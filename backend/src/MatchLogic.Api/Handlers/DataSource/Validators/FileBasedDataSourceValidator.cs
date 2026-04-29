using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Validators;

public class FileBasedDataSourceValidator : AbstractValidator<Guid>
{
    private readonly IGenericRepository<Domain.Import.FileImport, Guid> _fileImportRepository;
    public FileBasedDataSourceValidator(IGenericRepository<Domain.Import.FileImport, Guid> fileImportRepository)
    {
        _fileImportRepository = fileImportRepository;
        RuleFor(x => x)
            .NotNull()
            .NotEmpty()
            .NotEqual(Guid.Empty)
            .WithMessage(ValidationMessages.Required("File Id"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .WithName("FileId");

        RuleFor(x => x)
            .MustAsync(FileExists)
            .WithMessage(ValidationMessages.NotExists("File"))
            .WithErrorCode(ErrorCodeConstants.NotExists)
            .WithName("FileId");

        RuleFor(x => x)
            .MustAsync(FileDirectoryExists)
            .WithMessage(ValidationMessages.NotExists("Directory path"))
            .WithErrorCode(ErrorCodeConstants.NotExists)
            .WithName("FileId");

    }

    private async Task<bool> FileExists(Guid guid, CancellationToken cancellationToken)
    {
        var fileImport = await _fileImportRepository.GetByIdAsync(guid, Constants.Collections.ImportFile);
        return fileImport != null;
    }

    private async Task<bool> FileDirectoryExists(Guid guid, CancellationToken cancellationToken)
    {
        var fileImport = await _fileImportRepository.GetByIdAsync(guid, Constants.Collections.ImportFile);
        if (fileImport == null) return false;
        // M2: S3-uploaded files have an empty FilePath; S3Key is the source of truth.
        // ConfirmUploadHandler already verified the S3 object exists at upload time.
        if (!string.IsNullOrEmpty(fileImport.S3Key)) return true;
        // Legacy multipart-upload path: check the local directory still exists.
        return Path.Exists(fileImport.FilePath);
    }
}
