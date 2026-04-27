using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Project;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Validators;

public class ExcelConnectionValidator : AbstractValidator<BaseConnectionInfo>
{
    // Handle Excel specific validation logic
    public ExcelConnectionValidator(IGenericRepository<Domain.Import.FileImport, Guid> fileImportRepository) 
    {
        // Add any Excel specific validation rules here if needed
        RuleFor(x => x.Parameters)
            .MustAsync(IsValidExcelFile)
            .WithMessage("File is not a valid Excel file.")
            .WithErrorCode(ErrorCodeConstants.Required);
        //async Task<bool> IsValidExcelFile(Guid guid, CancellationToken cancellationToken)
        async Task<bool> IsValidExcelFile(Dictionary<string, string> parameters, CancellationToken cancellationToken)
        {
            Guid guid = parameters.TryGetValue("FileId", out var fileIdStr) && Guid.TryParse(fileIdStr, out var fileId) ? fileId : Guid.Empty;
            var fileImport = await fileImportRepository.GetByIdAsync(guid, Constants.Collections.ImportFile);
            // Check if the file exists and has a valid Excel extension
            if (fileImport == null) return false;
            // Check if the file extension is valid
            return ApiConstants.ExcelExtensions.Contains(fileImport.FileExtension);
        }
    }
}
