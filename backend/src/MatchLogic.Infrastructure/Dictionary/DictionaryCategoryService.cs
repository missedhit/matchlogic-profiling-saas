using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Dictionary;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Dictionary;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Dictionary;
public class DictionaryCategoryService : IDictionaryCategoryService
{
    private readonly IGenericRepository<DictionaryCategory, Guid> _dictionaryCategoryRepository;
    private readonly ILogger<DictionaryCategoryService> _logger;
    private readonly IConnectionBuilder _connectionBuilder;
    public DictionaryCategoryService(
        IGenericRepository<DictionaryCategory, Guid> dictionaryCategoryRepository,
        IConnectionBuilder connectionBuilder,
        ILogger<DictionaryCategoryService> logger)
    {
        _logger = logger;
        _dictionaryCategoryRepository = dictionaryCategoryRepository;
        _connectionBuilder = connectionBuilder;
    }

    public async Task<List<DictionaryCategory>> GetAllDictionaryCategories()
    {
        return await _dictionaryCategoryRepository.GetAllAsync(Constants.Collections.DictionaryCategory);
    }

    public async Task<DictionaryCategory> GetDictionaryCategoryById(Guid id)
    {
        return await _dictionaryCategoryRepository.GetByIdAsync(id, Constants.Collections.DictionaryCategory);
    }

    public async Task<DictionaryCategory> CreateDictionaryCategory(string name, string description, List<string> items)
    {
        var dictionaryCategory = new DictionaryCategory()
        {
            Name = name,
            Description = description,
            Items = items ?? new List<string>(),
            IsSystem = false,
            IsDeleted = false,
            Version = 1
        };
        _logger.LogInformation("Creating dictionary category: {Name}", name);
        await _dictionaryCategoryRepository.InsertAsync(dictionaryCategory, Constants.Collections.DictionaryCategory);

        return dictionaryCategory;
    }

    public async Task UpdateDictionaryCategory(DictionaryCategory dictionaryCategoryInput)
    {
        var dictionaryCategory = await _dictionaryCategoryRepository.GetByIdAsync(dictionaryCategoryInput.Id, Constants.Collections.DictionaryCategory);

        if (dictionaryCategory == null)
        {
            _logger.LogError("Dictionary category with ID {Id} not found", dictionaryCategoryInput.Id);
            throw new Exception("Dictionary category not found");
        }
        else if (dictionaryCategory.IsSystem)
        {
            _logger.LogError("Attempt to update system dictionary category with ID {Id}", dictionaryCategoryInput.Id);
            throw new Exception("Cannot update system dictionary category");
        }

        dictionaryCategory.Name = dictionaryCategoryInput.Name;
        dictionaryCategory.Description = dictionaryCategoryInput.Description;
        dictionaryCategory.Items = dictionaryCategoryInput.Items;

        await _dictionaryCategoryRepository.UpdateAsync(dictionaryCategory, Constants.Collections.DictionaryCategory);
    }

    public async Task DeleteDictionaryCategory(Guid id)
    {
        var dictionaryCategory = await _dictionaryCategoryRepository.GetByIdAsync(id, Constants.Collections.DictionaryCategory);

        if (dictionaryCategory == null)
        {
            _logger.LogError("Dictionary category with ID {Id} not found", id);
            throw new Exception("Dictionary category not found");
        }
        else if (dictionaryCategory.IsSystem)
        {
            _logger.LogError("Attempt to delete system dictionary category with ID {Id}", id);
            throw new Exception("Cannot delete system dictionary category");
        }
        
        await _dictionaryCategoryRepository.DeleteAsync(dictionaryCategory.Id, Constants.Collections.DictionaryCategory);
    }

    public async Task AddItemsToDictionaryCategory(Guid id, List<string> items)
    {
        var dictionaryCategory = await _dictionaryCategoryRepository.GetByIdAsync(id, Constants.Collections.DictionaryCategory);

        if (dictionaryCategory == null)
        {
            _logger.LogError("Dictionary category with ID {Id} not found", id);
            throw new Exception("Dictionary category not found");
        }

        // Add only unique items
        foreach (var item in items)
        {
            if (!dictionaryCategory.Items.Contains(item))
            {
                dictionaryCategory.Items.Add(item);
            }
        }

        await _dictionaryCategoryRepository.UpdateAsync(dictionaryCategory, Constants.Collections.DictionaryCategory);
    }

    public async Task RemoveItemsFromDictionaryCategory(Guid id, List<string> items)
    {
        var dictionaryCategory = await _dictionaryCategoryRepository.GetByIdAsync(id, Constants.Collections.DictionaryCategory);

        if (dictionaryCategory == null)
        {
            _logger.LogError("Dictionary category with ID {Id} not found", id);
            throw new Exception("Dictionary category not found");
        }
        else if (dictionaryCategory.IsSystem)
        {
            _logger.LogError("Attempt to modify system dictionary category with ID {Id}", id);
            throw new Exception("Cannot modify system dictionary category items");
        }

        foreach (var item in items)
        {
            dictionaryCategory.Items.Remove(item);
        }

        await _dictionaryCategoryRepository.UpdateAsync(dictionaryCategory, Constants.Collections.DictionaryCategory);
    }
    public async Task<DictionaryCategory> CreateDictionaryCategoryByFilePath(string name, string description, string filePath, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            _logger.LogError("Dictionary category name cannot be null or empty");
            throw new ArgumentException("Dictionary category name cannot be null or empty", nameof(name));
        }
        if (string.IsNullOrWhiteSpace(filePath))
        {
            _logger.LogError("File path cannot be null or empty");
            throw new ArgumentException("File path cannot be null or empty", nameof(filePath));
        }
        if (!File.Exists(filePath))
        {
            _logger.LogError("File not found at path: {FilePath}", filePath);
            throw new FileNotFoundException($"File not found at path: '{filePath}'", filePath);
        }
        if (!Path.GetExtension(filePath).Equals(".csv", StringComparison.CurrentCultureIgnoreCase))
        {
            _logger.LogError("File must be a CSV file: {FilePath}", filePath);
            throw new ArgumentException("File must be a CSV file", nameof(filePath));
        }
        _logger.LogInformation("Creating dictionary category from file: {FilePath}", filePath);
        var items = new List<string>();
        var parameters = new Dictionary<string, string>
        {
            { "HasHeaders", "false" },
            { "FilePath", filePath },
        };
        var reader = _connectionBuilder
            .WithArgs(Domain.Import.DataSourceType.CSV, parameters)
            .Build();
        var rowsEnumerable = await reader.ReadRowsAsync(1, cancellationToken);
        await foreach (Dictionary<string, object> row in rowsEnumerable.WithCancellation(cancellationToken))
        {
            foreach (var value in row.Values)
            {
                if (value != null)
                {
                    items.Add(value.ToString());
                }
            }
            // If still empty, assume first row as values
            if (items.Count == 0)
                items.AddRange(row.Keys.ToList());

        }

        if (items.Count == 0)
        {
            _logger.LogError("File contains no data: {filePath}", filePath);
            throw new Exception("File contains no data");
        }

        // Ensure uniqueness
        var duplicate = items
            .GroupBy(x => x)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .FirstOrDefault();

        if (duplicate != null)
        {
            _logger.LogError("Duplicate item found in category items: {Duplicate}", duplicate);
            throw new Exception($"Duplicate item found in category items: '{duplicate}'");
        }

        return await CreateDictionaryCategory(name, description, items);
    }
}