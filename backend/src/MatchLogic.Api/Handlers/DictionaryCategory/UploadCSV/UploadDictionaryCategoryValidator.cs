using FluentValidation;
using Microsoft.AspNetCore.Http;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using System;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.Handlers.DictionaryCategory.UploadCSV;

public class UploadDictionaryCategoryValidator : AbstractValidator<UploadDictionaryCategoryRequest>
{
    private readonly string[] AllowedExtensions = { ".csv" };

    private readonly string[] AllowedMimeTypes = { "text/csv" };
    public UploadDictionaryCategoryValidator(IGenericRepository<Domain.Dictionary.DictionaryCategory, Guid> dictionaryRepository)
    {
        RuleFor(x => x.File)
         .NotNull().WithMessage(ValidationMessages.NotUploaded("file"))
         .Must(file => file?.Length > 0).WithMessage(ValidationMessages.IsEmpty("File"))

         .Must(HaveValidExtension).WithMessage(ValidationMessages.NotAllowedWithList("File", string.Join(", ", AllowedExtensions)))
         //.Must(HaveValidExtension).WithMessage($"File extension is not allowed. Allowed extensions are: {string.Join(", ", AllowedExtensions)}")
         //.Must(file => file.Length <= MaxFileSize).WithMessage($"File size must be less than {MaxFileSize / (1024 * 1024)} MB.")
         .Must(HaveValidMimeType).WithMessage(ValidationMessages.Invalid("Mime type"));


        RuleFor(x => x.Name)
            .NotEmpty().WithMessage(ValidationMessages.Required("Name"))
            .MaximumLength(Constants.FieldLength.NameMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Name", Constants.FieldLength.NameMaxLength))
            .MustAsync(BeUniqueName).WithMessage(ValidationMessages.AlreadyExists("dictionary category"));

        RuleFor(x => x.Description)
            .MaximumLength(Constants.FieldLength.DescriptionMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Description", Constants.FieldLength.DescriptionMaxLength));


        /* RuleFor(x => x.Items)
             .NotNull().WithMessage("Items list cannot be null.")
             .NotEmpty().WithMessage("Items list cannot be empty.")
             .Must(items => items.All(item => !string.IsNullOrWhiteSpace(item)))
             .WithMessage("Items list cannot contain empty or whitespace values.")
             .Must(items => items.Distinct().Count() == items.Count)
             .WithMessage("Items list must contain unique values.");*/

        async Task<bool> BeUniqueName(string name, CancellationToken cancellationToken)
        {
            var existing = await dictionaryRepository.QueryAsync(x => x.Name == name, Constants.Collections.DictionaryCategory);
            return existing == null || existing?.Count == 0;
        }
    }
    private bool HaveValidExtension(IFormFile file)
    {
        if (file == null) return false;
        string ext = Path.GetExtension(file.FileName).ToLowerInvariant();
        return AllowedExtensions.Contains(ext);
    }

    private bool HaveValidMimeType(IFormFile file)
    {
        if (file == null) return false;

        return AllowedMimeTypes.Contains(file.ContentType);
    }
}
