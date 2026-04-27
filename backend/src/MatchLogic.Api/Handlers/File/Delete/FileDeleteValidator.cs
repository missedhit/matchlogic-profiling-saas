using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.File.Delete;

public class FileDeleteValidator : AbstractValidator<FileDeleteRequest>
{
    public FileDeleteValidator(IGenericRepository<Domain.Import.FileImport, Guid> fileImportRepository)
    {
        RuleLevelCascadeMode = ClassLevelCascadeMode;
        RuleFor(x => x.FileId)
            .NotNull()
            .NotEmpty()
            .NotEqual(Guid.Empty)
            .WithMessage(ValidationMessages.Required("File Id")).WithErrorCode(ErrorCodeConstants.Required);


        RuleFor(x => x.FileId).
            MustAsync(FileExists)
            .WithMessage(ValidationMessages.NotExists("File"))
            .WithErrorCode(ErrorCodeConstants.NotExists);

        async Task<bool> FileExists(Guid guid, CancellationToken cancellationToken)
        {
            var file = await fileImportRepository.GetByIdAsync(guid, Constants.Collections.ImportFile);
            return file != null;
        }
    }
}
