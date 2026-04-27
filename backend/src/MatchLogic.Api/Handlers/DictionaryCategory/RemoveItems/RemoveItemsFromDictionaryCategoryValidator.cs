using FluentValidation;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Common;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.Handlers.DictionaryCategory.RemoveItems;

public class RemoveItemsFromDictionaryCategoryValidator : AbstractValidator<RemoveItemsFromDictionaryCategoryRequest>
{
    public RemoveItemsFromDictionaryCategoryValidator(IGenericRepository<Domain.Dictionary.DictionaryCategory, Guid> dictionaryRepository)
    {

        RuleFor(x => x.Id)
            .NotEmpty().WithMessage(ValidationMessages.Required("Id"))
            .MustAsync(ExistInDatabase).WithMessage(ValidationMessages.NotExists("Dictionary"))
            .WithErrorCode(ErrorCodeConstants.NotExists)
            .MustAsync(IsSystemPattern).WithMessage(ValidationMessages.ModificationNotAllowed("System Dictionary"))
            .WithErrorCode(ErrorCodeConstants.Error);

        RuleFor(x => x.Items)
            .NotNull().WithMessage(ValidationMessages.CannotBeNull("Items list"))
            .NotEmpty().WithMessage(ValidationMessages.CannotBeEmpty("Items list"))
            .Must(items => items.All(item => !string.IsNullOrWhiteSpace(item)))
            .WithMessage(ValidationMessages.CannotContainEmptyOrWhitespace("Items list"))
            .Must(items => items.Distinct().Count() == items.Count)
            .WithMessage(ValidationMessages.MustBeUnique("Items list"));

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
