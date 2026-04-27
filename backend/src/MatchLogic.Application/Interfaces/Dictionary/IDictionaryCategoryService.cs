using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Dictionary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Dictionary
{
    public interface IDictionaryCategoryService
    {
        Task<List<DictionaryCategory>> GetAllDictionaryCategories();
        Task<DictionaryCategory> GetDictionaryCategoryById(Guid id);
        Task<DictionaryCategory> CreateDictionaryCategory(string name, string description, List<string> items);
        Task<DictionaryCategory> CreateDictionaryCategoryByFilePath(string name, string description, string filePath, CancellationToken cancellationToken);
        Task UpdateDictionaryCategory(DictionaryCategory dictionaryCategory);
        Task DeleteDictionaryCategory(Guid id);
        Task AddItemsToDictionaryCategory(Guid id, List<string> items);
        Task RemoveItemsFromDictionaryCategory(Guid id, List<string> items);
    }
}
