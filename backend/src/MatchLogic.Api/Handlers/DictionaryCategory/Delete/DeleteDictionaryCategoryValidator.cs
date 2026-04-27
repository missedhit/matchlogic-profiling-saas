using FluentValidation;
using System.Threading.Tasks;
using System;
using MatchLogic.Application.Interfaces.Persistence;
using DomainModel = MatchLogic.Domain.Dictionary.DictionaryCategory;
using MatchLogic.Application.Common;
using MatchLogic.Api.Common;
using System.Threading;
namespace MatchLogic.Api.Handlers.DictionaryCategory.Delete;

public class DeleteDictionaryCategoryValidator : AbstractValidator<DeleteDictionaryCategoryRequest>
{
    public DeleteDictionaryCategoryValidator(IGenericRepository<DomainModel, Guid> dictionaryRepository)
    {
        RuleFor(x => x.Id)
            .NotEmpty().WithMessage(ValidationMessages.Required("Id"))
            .MustAsync(ExistInDatabase).WithMessage(ValidationMessages.NotExists("Dictionary"))
            .WithErrorCode(ErrorCodeConstants.NotExists)
            .MustAsync(IsSystemPattern).WithMessage("System Dictionary cannot be deleted.")
            .WithErrorCode(ErrorCodeConstants.Error);

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
