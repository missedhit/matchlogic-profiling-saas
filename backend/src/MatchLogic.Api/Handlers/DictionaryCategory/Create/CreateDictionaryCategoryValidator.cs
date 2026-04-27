using FluentValidation;
using System.Linq;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using System;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.Handlers.DictionaryCategory.Create;

public class CreateDictionaryCategoryValidator : AbstractValidator<CreateDictionaryCategoryRequest>
{
    public CreateDictionaryCategoryValidator(IGenericRepository<Domain.Dictionary.DictionaryCategory, Guid> dictionaryRepository)
    {
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
    }
}
