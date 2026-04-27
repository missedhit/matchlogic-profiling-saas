using FluentValidation;
using System.Linq;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using DomainModel = MatchLogic.Domain.Dictionary.DictionaryCategory;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Api.Common;
namespace MatchLogic.Api.Handlers.DictionaryCategory.Update;

public class UpdateDictionaryCategoryValidator : AbstractValidator<UpdateDictionaryCategoryRequest>
{
    public UpdateDictionaryCategoryValidator(IGenericRepository<DomainModel, Guid> dictionaryRepository)
    {
        RuleFor(x => x.Id)
            .NotEmpty().WithMessage(ValidationMessages.Required("Id"))
            .MustAsync(ExistInDatabase).WithMessage(ValidationMessages.NotExists("Dictionary"))
            .WithErrorCode(ErrorCodeConstants.NotExists)
            .MustAsync(IsSystemPattern).WithMessage("System Dictionary cannot be updated.")
            .WithErrorCode(ErrorCodeConstants.Error);

        RuleFor(x => x.Name)
            .NotEmpty().WithMessage(ValidationMessages.Required("Name"))
            .MaximumLength(Constants.FieldLength.NameMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Name", Constants.FieldLength.NameMaxLength))
            .MustAsync(BeUniqueName).WithMessage(ValidationMessages.AlreadyExists("dictionary category"));

        RuleFor(x => x.Description)
            .MaximumLength(Constants.FieldLength.DescriptionMaxLength)
            .WithMessage(ValidationMessages.MaxLength("Description", Constants.FieldLength.DescriptionMaxLength));

        RuleFor(x => x.Items)
            .NotNull().WithMessage(ValidationMessages.CannotBeNull("Items list"))
            .NotEmpty().WithMessage(ValidationMessages.CannotBeEmpty("Items list"))
            .Must(items => items.All(item => !string.IsNullOrWhiteSpace(item)))
            .WithMessage(ValidationMessages.CannotContainEmptyOrWhitespace("Items list"))
            .Must(items => items.Distinct().Count() == items.Count)
            .WithMessage(ValidationMessages.MustBeUnique("Items list"));


        async Task<bool> BeUniqueName(string name, CancellationToken cancellationToken)
        {
            var existing = await dictionaryRepository.QueryAsync(x => x.Name == name, Constants.Collections.DictionaryCategory);
            return existing == null || existing?.Count == 0;
        }

        // Check if the regex pattern exists
        async Task<bool> ExistInDatabase(Guid id, CancellationToken cancellationToken)
        {
            var pattern = await dictionaryRepository.GetByIdAsync(id, Constants.Collections.DictionaryCategory);
            return pattern != null;
        }

        // Check if it's a system pattern which shouldn't be deleted
        async Task<bool> IsSystemPattern(Guid id, CancellationToken cancellationToken)
        {
            var pattern = await dictionaryRepository.GetByIdAsync(id, Constants.Collections.DictionaryCategory);
            return pattern != null && !pattern.IsSystem; // System patterns can't be updated
        }
    }
}
