using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Validators;

public class CSVConnectionValidator : AbstractValidator<BaseConnectionInfo>
{
    private static readonly string[] LineEndings = ["\r", "\n", "\r\n"];
    private const string QuoteKey = "Qoute";
    private const string DelimiterKey = "Delimiter";

    public CSVConnectionValidator(IGenericRepository<FileImport, Guid> fileImportRepository)
    {
       
        RuleFor(x => x)
            .NotNull()
            .WithMessage(ValidationMessages.CannotBeNull("Connection"))
            .WithErrorCode(ErrorCodeConstants.Required)
            .WithName("Connection");

        RuleFor(x => x.Parameters)
            .MustAsync(IsValidExcelFile)
            .WithMessage("File is not a valid CSV file.")
            .WithErrorCode(ErrorCodeConstants.Required);


        async Task<bool> IsValidExcelFile(Dictionary<string, string> parameters, CancellationToken cancellationToken)
        {
            Guid guid = parameters.TryGetValue("FileId",out var fileIdStr) && Guid.TryParse(fileIdStr,out var fileId) ? fileId : Guid.Empty;
            var fileImport = await fileImportRepository.GetByIdAsync(guid, Constants.Collections.ImportFile);
            // Check if the file exists and has a valid extension
            if (fileImport == null) return false;
            // Check if the file extension is valid
            return ApiConstants.CsvExtensions.Contains(fileImport.FileExtension);
        }

        // Combined validation for Quote parameter
        RuleFor(x => x.Parameters)
            .Must(ValidateQuoteParameter)
            .WithMessage(GetQuoteValidationMessage);

        // Combined validation for Delimiter parameter  
        RuleFor(x => x.Parameters)
            .Must(ValidateDelimiterParameter)
            .WithMessage(GetDelimiterValidationMessage);

        // Validate Quote and Delimiter are different
        RuleFor(x => x.Parameters)
            .Must(ValidateQuoteDelimiterDifference)
            .WithMessage($"The {QuoteKey} character and the {DelimiterKey} cannot be the same.");

        
    }

   
    private static bool ValidateQuoteParameter(Dictionary<string, string> parameters)
    {
        return ValidateKeyParameter(parameters, QuoteKey);
    }

    private static bool ValidateDelimiterParameter(Dictionary<string, string> parameters)
    {
        return ValidateKeyParameter(parameters, DelimiterKey);
    }
    
    private static string GetQuoteValidationMessage( BaseConnectionInfo connectionInfo)
    {
        return GetKeyValidationMessage(connectionInfo.Parameters, QuoteKey);
    }

    private static string GetDelimiterValidationMessage(BaseConnectionInfo connectionInfo)
    {
        return GetKeyValidationMessage(connectionInfo.Parameters, DelimiterKey);
    }

    private static bool ValidateKeyParameter(Dictionary<string, string> parameters, string Key)
    {
        if (!parameters.TryGetValue(Key, out var keyVal))
            return true;

        return !string.IsNullOrEmpty(keyVal) &&
               !keyVal.Equals(" ") && 
               keyVal.Length == 1 &&
               !LineEndings.Contains(keyVal);
    }
    private static string GetKeyValidationMessage(Dictionary<string, string> parameters, string Key)
    {
        if (!parameters.TryGetValue(Key, out var keyVal))
            return string.Empty;

        if (string.IsNullOrEmpty(keyVal))
            return $"The {Key} character '{keyVal}' cannot be Empty or null.";

        if (keyVal.Equals(" "))
            return $"The {Key} character '{keyVal}' cannot be a WhiteSpaceChar.";

        if (keyVal.Length != 1)
            return $"The {Key} must be a single character.";

        return $"The {Key} '{keyVal}' cannot be a line ending. ({string.Join(',', LineEndings)})";
    }

    private static bool ValidateQuoteDelimiterDifference(Dictionary<string, string> parameters)
    {
        return !(parameters.TryGetValue(DelimiterKey, out var delimiter) &&
                 parameters.TryGetValue(QuoteKey, out var quote) &&
                 string.Equals(delimiter, quote, StringComparison.Ordinal));
    }
}
