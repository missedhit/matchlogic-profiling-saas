using MatchLogic.Application.Interfaces.Dictionary;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;
using DomainDictionaryCategory = MatchLogic.Domain.Dictionary.DictionaryCategory;

namespace MatchLogic.Api.IntegrationTests.Builders;

public class DictionaryCategoryBuilder(IServiceProvider serviceProvider)
{
    private readonly DomainDictionaryCategory _dictionaryCategory = new()
    {
        Name = "Test Dictionary Category",
        Description = "Test Dictionary Category Description",
        Items = ["Item1", "Item2", "Item3"],
        IsDefault = false,
        IsSystem = false,
        IsDeleted = false
    };

    private readonly IServiceProvider _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));

    public DictionaryCategoryBuilder WithValid()
    {
        _dictionaryCategory.Name = "Test Dictionary Category";
        _dictionaryCategory.Description = "Test Dictionary Category Description";
        _dictionaryCategory.Items = ["Item1", "Item2", "Item3"];
        _dictionaryCategory.IsDefault = false;
        _dictionaryCategory.IsSystem = false;
        _dictionaryCategory.IsDeleted = false;
        return this;
    }

    public DictionaryCategoryBuilder WithId(Guid id)
    {
        _dictionaryCategory.Id = id;
        return this;
    }

    public DictionaryCategoryBuilder WithName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Name cannot be null or empty.", nameof(name));
        _dictionaryCategory.Name = name;
        return this;
    }

    public DictionaryCategoryBuilder WithDescription(string description)
    {
        if (string.IsNullOrWhiteSpace(description))
            throw new ArgumentException("Description cannot be null or empty.", nameof(description));
        _dictionaryCategory.Description = description;
        return this;
    }

    public DictionaryCategoryBuilder WithItems(List<string> items)
    {
        _dictionaryCategory.Items = items ?? new List<string>();
        return this;
    }

    public DictionaryCategoryBuilder WithIsDefault(bool isDefault)
    {
        _dictionaryCategory.IsDefault = isDefault;
        return this;
    }

    public DictionaryCategoryBuilder WithIsSystem(bool isSystem)
    {
        _dictionaryCategory.IsSystem = isSystem;
        return this;
    }

    public DomainDictionaryCategory BuildDomain() => _dictionaryCategory;

    public async Task<DomainDictionaryCategory> BuildAsync()
    {
        //var service = _serviceProvider.GetService<IDictionaryCategoryService>();
        //return await service.CreateDictionaryCategory(
        //    _dictionaryCategory.Name,
        //    _dictionaryCategory.Description,
        //    _dictionaryCategory.Items);

        var _dictionaryCategoryRepository = _serviceProvider.GetService<IGenericRepository<DomainDictionaryCategory, Guid>>();
        await _dictionaryCategoryRepository.InsertAsync(_dictionaryCategory, MatchLogic.Application.Common.Constants.Collections.DictionaryCategory);
        return _dictionaryCategory;
    }
}
