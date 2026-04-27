using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Dictionary;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Dictionary;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchLogic.Application.UnitTests
{
    public class DictionaryCategoryServiceTests
    {
        private readonly Mock<IGenericRepository<DictionaryCategory, Guid>> _mockRepository;
        private readonly Mock<ILogger<DictionaryCategoryService>> _mockLogger;
        private readonly Mock<IConnectionBuilder> _dataReader;
        private readonly DictionaryCategoryService _service;

        public DictionaryCategoryServiceTests()
        {
            _mockRepository = new Mock<IGenericRepository<DictionaryCategory, Guid>>();
            _mockLogger = new Mock<ILogger<DictionaryCategoryService>>();
            _dataReader = new Mock<IConnectionBuilder>();
            _service = new DictionaryCategoryService(_mockRepository.Object, _dataReader.Object, _mockLogger.Object);
        }

        #region GetAllDictionaryCategories Tests

        [Fact]
        public async Task GetAllDictionaryCategories_ShouldReturnAllCategories()
        {
            // Arrange
            var expectedCategories = new List<DictionaryCategory>
            {
                new DictionaryCategory { Id = Guid.NewGuid(), Name = "Cities" },
                new DictionaryCategory { Id = Guid.NewGuid(), Name = "States" }
            };

            _mockRepository
                .Setup(repo => repo.GetAllAsync(Constants.Collections.DictionaryCategory))
                .ReturnsAsync(expectedCategories);

            // Act
            var result = await _service.GetAllDictionaryCategories();

            // Assert
            Assert.Equal(expectedCategories.Count, result.Count);
            Assert.Equal(expectedCategories, result);
            _mockRepository.Verify(repo => repo.GetAllAsync(Constants.Collections.DictionaryCategory), Times.Once);
        }

        [Fact]
        public async Task GetAllDictionaryCategories_WhenNoCategories_ShouldReturnEmptyList()
        {
            // Arrange
            _mockRepository
                .Setup(repo => repo.GetAllAsync(Constants.Collections.DictionaryCategory))
                .ReturnsAsync(new List<DictionaryCategory>());

            // Act
            var result = await _service.GetAllDictionaryCategories();

            // Assert
            Assert.Empty(result);
            _mockRepository.Verify(repo => repo.GetAllAsync(Constants.Collections.DictionaryCategory), Times.Once);
        }

        #endregion

        #region GetDictionaryCategoryById Tests

        [Fact]
        public async Task GetDictionaryCategoryById_WithValidId_ShouldReturnCategory()
        {
            // Arrange
            var id = Guid.NewGuid();
            var expectedCategory = new DictionaryCategory { Id = id, Name = "Cities" };

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(expectedCategory);

            // Act
            var result = await _service.GetDictionaryCategoryById(id);

            // Assert
            Assert.Equal(expectedCategory, result);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
        }

        [Fact]
        public async Task GetDictionaryCategoryById_WithInvalidId_ShouldReturnNull()
        {
            // Arrange
            var id = Guid.NewGuid();

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync((DictionaryCategory)null);

            // Act
            var result = await _service.GetDictionaryCategoryById(id);

            // Assert
            Assert.Null(result);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
        }

        #endregion

        #region CreateDictionaryCategory Tests

        [Fact]
        public async Task CreateDictionaryCategory_ShouldCreateAndReturnNewCategory()
        {
            // Arrange
            var name = "Cities";
            var description = "List of cities";
            var items = new List<string> { "New York", "Los Angeles", "Chicago" };

            _mockRepository
                .Setup(repo => repo.InsertAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory));

            // Act
            var result = await _service.CreateDictionaryCategory(name, description, items);

            // Assert
            Assert.Equal(name, result.Name);
            Assert.Equal(description, result.Description);
            Assert.Equal(items, result.Items);
            Assert.False(result.IsSystem);
            Assert.False(result.IsDeleted);
            Assert.Equal(1, result.Version);

            _mockRepository.Verify(repo => repo.InsertAsync(
                It.Is<DictionaryCategory>(c =>
                    c.Name == name &&
                    c.Description == description &&
                    c.Items == items),
                Constants.Collections.DictionaryCategory),
                Times.Once);
        }

        [Fact]
        public async Task CreateDictionaryCategory_WithNullItems_ShouldCreateWithEmptyItemsList()
        {
            // Arrange
            var name = "Cities";
            var description = "List of cities";
            List<string> items = null;

            _mockRepository
                .Setup(repo => repo.InsertAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory));

            // Act
            var result = await _service.CreateDictionaryCategory(name, description, items);

            // Assert
            Assert.Equal(name, result.Name);
            Assert.Equal(description, result.Description);
            Assert.NotNull(result.Items);
            Assert.Empty(result.Items);

            _mockRepository.Verify(repo => repo.InsertAsync(
                It.Is<DictionaryCategory>(c =>
                    c.Name == name &&
                    c.Description == description &&
                    c.Items != null &&
                    !c.Items.Any()),
                Constants.Collections.DictionaryCategory),
                Times.Once);
        }

        #endregion

        #region UpdateDictionaryCategory Tests

        [Fact]
        public async Task UpdateDictionaryCategory_WithValidCategory_ShouldUpdateCategory()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingCategory = new DictionaryCategory
            {
                Id = id,
                Name = "Old Name",
                Description = "Old Description",
                Items = new List<string> { "Old Item" },
                IsSystem = false
            };

            var updatedCategory = new DictionaryCategory
            {
                Id = id,
                Name = "New Name",
                Description = "New Description",
                Items = new List<string> { "New Item" }
            };

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(existingCategory);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory));

            // Act
            await _service.UpdateDictionaryCategory(updatedCategory);

            // Assert
            Assert.Equal(updatedCategory.Name, existingCategory.Name);
            Assert.Equal(updatedCategory.Description, existingCategory.Description);
            Assert.Equal(updatedCategory.Items, existingCategory.Items);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(
                It.Is<DictionaryCategory>(c =>
                    c.Id == id &&
                    c.Name == updatedCategory.Name &&
                    c.Description == updatedCategory.Description &&
                    c.Items == updatedCategory.Items),
                Constants.Collections.DictionaryCategory),
                Times.Once);
        }

        [Fact]
        public async Task UpdateDictionaryCategory_WithNonExistentCategory_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();
            var updatedCategory = new DictionaryCategory
            {
                Id = id,
                Name = "New Name",
                Description = "New Description",
                Items = new List<string> { "New Item" }
            };

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync((DictionaryCategory)null);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.UpdateDictionaryCategory(updatedCategory));
            Assert.Equal("Dictionary category not found", exception.Message);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory), Times.Never);
        }

        [Fact]
        public async Task UpdateDictionaryCategory_WithSystemCategory_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingCategory = new DictionaryCategory
            {
                Id = id,
                Name = "System Category",
                IsSystem = true
            };

            var updatedCategory = new DictionaryCategory
            {
                Id = id,
                Name = "New Name"
            };

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(existingCategory);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.UpdateDictionaryCategory(updatedCategory));
            Assert.Equal("Cannot update system dictionary category", exception.Message);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory), Times.Never);
        }

        #endregion

        #region DeleteDictionaryCategory Tests

        [Fact]
        public async Task DeleteDictionaryCategory_WithValidId_ShouldMarkAsDeleted()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingCategory = new DictionaryCategory
            {
                Id = id,
                Name = "Test Category",
                IsDeleted = false,
                IsSystem = false
            };

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(existingCategory);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory));

            // Act
            await _service.DeleteDictionaryCategory(id);

            // Assert
            Assert.True(existingCategory.IsDeleted);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(
                It.Is<DictionaryCategory>(c => c.Id == id && c.IsDeleted),
                Constants.Collections.DictionaryCategory),
                Times.Once);
        }

        [Fact]
        public async Task DeleteDictionaryCategory_WithNonExistentId_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync((DictionaryCategory)null);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.DeleteDictionaryCategory(id));
            Assert.Equal("Dictionary category not found", exception.Message);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory), Times.Never);
        }

        [Fact]
        public async Task DeleteDictionaryCategory_WithSystemCategory_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingCategory = new DictionaryCategory
            {
                Id = id,
                Name = "System Category",
                IsSystem = true
            };

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(existingCategory);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.DeleteDictionaryCategory(id));
            Assert.Equal("Cannot delete system dictionary category", exception.Message);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory), Times.Never);
        }

        #endregion

        #region AddItemsToDictionaryCategory Tests

        [Fact]
        public async Task AddItemsToDictionaryCategory_WithValidIdAndItems_ShouldAddItems()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingCategory = new DictionaryCategory
            {
                Id = id,
                Name = "Cities",
                Items = new List<string> { "New York", "Los Angeles" }
            };

            var itemsToAdd = new List<string> { "Chicago", "Houston", "New York" }; // Note: "New York" is already in the list

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(existingCategory);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory));

            // Act
            await _service.AddItemsToDictionaryCategory(id, itemsToAdd);

            // Assert
            // Should contain the original items plus new unique items
            Assert.Equal(4, existingCategory.Items.Count);
            Assert.Contains("New York", existingCategory.Items);
            Assert.Contains("Los Angeles", existingCategory.Items);
            Assert.Contains("Chicago", existingCategory.Items);
            Assert.Contains("Houston", existingCategory.Items);
            // Should only appear once
            Assert.Equal(1, existingCategory.Items.Count(i => i == "New York"));

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(
                It.Is<DictionaryCategory>(c =>
                    c.Id == id &&
                    c.Items.Count == 4 &&
                    c.Items.Contains("Chicago") &&
                    c.Items.Contains("Houston")),
                Constants.Collections.DictionaryCategory),
                Times.Once);
        }

        [Fact]
        public async Task AddItemsToDictionaryCategory_WithNonExistentId_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();
            var itemsToAdd = new List<string> { "Chicago", "Houston" };

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync((DictionaryCategory)null);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.AddItemsToDictionaryCategory(id, itemsToAdd));
            Assert.Equal("Dictionary category not found", exception.Message);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory), Times.Never);
        }

        [Fact]
        public async Task AddItemsToDictionaryCategory_WithEmptyItemsList_ShouldNotModifyCategory()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingCategory = new DictionaryCategory
            {
                Id = id,
                Name = "Cities",
                Items = new List<string> { "New York", "Los Angeles" }
            };

            var originalItems = new List<string>(existingCategory.Items);
            var itemsToAdd = new List<string>(); // Empty list

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(existingCategory);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory));

            // Act
            await _service.AddItemsToDictionaryCategory(id, itemsToAdd);

            // Assert
            Assert.Equal(originalItems.Count, existingCategory.Items.Count);
            Assert.Equal(originalItems, existingCategory.Items);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(
                It.Is<DictionaryCategory>(c => c.Id == id),
                Constants.Collections.DictionaryCategory),
                Times.Once);
        }

        #endregion

        #region RemoveItemsFromDictionaryCategory Tests

        [Fact]
        public async Task RemoveItemsFromDictionaryCategory_WithValidIdAndItems_ShouldRemoveItems()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingCategory = new DictionaryCategory
            {
                Id = id,
                Name = "Cities",
                Items = new List<string> { "New York", "Los Angeles", "Chicago", "Houston" },
                IsSystem = false
            };

            var itemsToRemove = new List<string> { "Chicago", "Houston", "Dallas" }; // Note: "Dallas" is not in the list

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(existingCategory);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory));

            // Act
            await _service.RemoveItemsFromDictionaryCategory(id, itemsToRemove);

            // Assert
            Assert.Equal(2, existingCategory.Items.Count);
            Assert.Contains("New York", existingCategory.Items);
            Assert.Contains("Los Angeles", existingCategory.Items);
            Assert.DoesNotContain("Chicago", existingCategory.Items);
            Assert.DoesNotContain("Houston", existingCategory.Items);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(
                It.Is<DictionaryCategory>(c =>
                    c.Id == id &&
                    c.Items.Count == 2 &&
                    !c.Items.Contains("Chicago") &&
                    !c.Items.Contains("Houston")),
                Constants.Collections.DictionaryCategory),
                Times.Once);
        }

        [Fact]
        public async Task RemoveItemsFromDictionaryCategory_WithNonExistentId_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();
            var itemsToRemove = new List<string> { "Chicago", "Houston" };

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync((DictionaryCategory)null);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.RemoveItemsFromDictionaryCategory(id, itemsToRemove));
            Assert.Equal("Dictionary category not found", exception.Message);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory), Times.Never);
        }

        [Fact]
        public async Task RemoveItemsFromDictionaryCategory_WithSystemCategory_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingCategory = new DictionaryCategory
            {
                Id = id,
                Name = "System Category",
                Items = new List<string> { "Item1", "Item2" },
                IsSystem = true
            };

            var itemsToRemove = new List<string> { "Item1" };

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(existingCategory);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.RemoveItemsFromDictionaryCategory(id, itemsToRemove));
            Assert.Equal("Cannot modify system dictionary category items", exception.Message);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory), Times.Never);
        }

        [Fact]
        public async Task RemoveItemsFromDictionaryCategory_WithEmptyItemsList_ShouldNotModifyCategory()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingCategory = new DictionaryCategory
            {
                Id = id,
                Name = "Cities",
                Items = new List<string> { "New York", "Los Angeles" },
                IsSystem = false
            };

            var originalItems = new List<string>(existingCategory.Items);
            var itemsToRemove = new List<string>(); // Empty list

            _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory))
                .ReturnsAsync(existingCategory);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory));

            // Act
            await _service.RemoveItemsFromDictionaryCategory(id, itemsToRemove);

            // Assert
            Assert.Equal(originalItems.Count, existingCategory.Items.Count);
            Assert.Equal(originalItems, existingCategory.Items);

            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.DictionaryCategory), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(
                It.Is<DictionaryCategory>(c => c.Id == id),
                Constants.Collections.DictionaryCategory),
                Times.Once);
        }

        #endregion

        #region CreateDictionaryCategoryByFilePath Tests

        [Fact]
        public async Task CreateDictionaryCategoryByFilePath_ShouldCreateAndReturnNewCategory()
        {
            // Arrange
            var name = "Cities";
            var description = "List of cities";
            var filePath = CreateSampleCsvFile();
            var cancellationToken = CancellationToken.None;

            var expectedCategory = new DictionaryCategory
            {
                Id = Guid.NewGuid(),
                Name = name,
                Description = description,
                Items = ["New York", "Los Angeles", "Chicago"],
                IsSystem = false,
                IsDeleted = false,
                Version = 1
            };

            var mockStrategy = new Mock<IConnectionReaderStrategy>();
            mockStrategy
                .Setup(strategy => strategy.ReadRowsAsync(It.IsAny<int>(), cancellationToken))
                .ReturnsAsync(new List<IDictionary<string, object>>
                {
                    new Dictionary<string, object> { { "City", "New York" } },
                    new Dictionary<string, object> { { "City", "Los Angeles" } },
                    new Dictionary<string, object> { { "City", "Chicago" } }
                }.ToAsyncEnumerable());

            _dataReader
                .Setup(builder => builder.WithArgs(DataSourceType.CSV, It.IsAny<Dictionary<string, string>>(), null))
                .Returns(_dataReader.Object);

            _dataReader
                .Setup(builder => builder.Build())
                .Returns(mockStrategy.Object);

            _mockRepository
                .Setup(repo => repo.InsertAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory));

            // Act
            var result = await _service.CreateDictionaryCategoryByFilePath(name, description, filePath, cancellationToken);

            // Assert
            Assert.Equal(name, result.Name);
            Assert.Equal(description, result.Description);
            Assert.Equal(expectedCategory.Items, result.Items);
            Assert.False(result.IsSystem);
            Assert.False(result.IsDeleted);
            Assert.Equal(1, result.Version);

            _mockRepository.Verify(repo => repo.InsertAsync(
                It.Is<DictionaryCategory>(c =>
                    c.Name == name &&
                    c.Description == description &&
                    c.Items.SequenceEqual(expectedCategory.Items)),
                Constants.Collections.DictionaryCategory),
                Times.Once);
        }

        [Fact]
        public async Task CreateDictionaryCategoryByFilePath_WithInvalidFilePath_ShouldThrowException()
        {
            // Arrange
            var name = "Cities";
            var description = "List of cities";
            var filePath = "invalid/path/to/file.csv";
            var cancellationToken = CancellationToken.None;

            _dataReader
                .Setup(builder => builder.WithArgs(DataSourceType.CSV, It.IsAny<Dictionary<string, string>>(), null))
                .Throws(new FileNotFoundException("File not found"));

            // Act & Assert
            var exception = await Assert.ThrowsAsync<FileNotFoundException>(() => _service.CreateDictionaryCategoryByFilePath(name, description, filePath, cancellationToken));
            Assert.Contains("File not found", exception.Message);

            _mockRepository.Verify(repo => repo.InsertAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory), Times.Never);
        }

        [Fact]
        public async Task CreateDictionaryCategoryByFilePath_WithEmptyFile_ShouldThrowException()
        {
            // Arrange
            var name = "Cities";
            var description = "List of cities";
            var filePath = CreateEmptyCsvFile();
            var cancellationToken = CancellationToken.None;

            var mockStrategy = new Mock<IConnectionReaderStrategy>();
            mockStrategy
                .Setup(strategy => strategy.ReadRowsAsync(It.IsAny<int>(), cancellationToken))
                .ReturnsAsync(AsyncEnumerable.Empty<IDictionary<string, object>>());

            _dataReader
                .Setup(builder => builder.WithArgs(DataSourceType.CSV, It.IsAny<Dictionary<string, string>>(), null))
                .Returns(_dataReader.Object);

            _dataReader
                .Setup(builder => builder.Build())
                .Returns(mockStrategy.Object);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.CreateDictionaryCategoryByFilePath(name, description, filePath, cancellationToken));
            Assert.Contains("File contains no data", exception.Message);

            _mockRepository.Verify(repo => repo.InsertAsync(It.IsAny<DictionaryCategory>(), Constants.Collections.DictionaryCategory), Times.Never);
        }
        #region Helper Methods

        private string CreateTempCsvFile(string fileName, IEnumerable<string> headers, IEnumerable<IEnumerable<string>> rows)
        {
            // Create a temporary directory if it doesn't exist
            var tempDirectory = Path.GetTempPath();
            var filePath = Path.Combine(tempDirectory, fileName);

            using (var writer = new StreamWriter(filePath))
            {
                // Write headers
                writer.WriteLine(string.Join(",", headers));

                // Write rows
                foreach (var row in rows)
                {
                    writer.WriteLine(string.Join(",", row));
                }
            }

            return filePath;
        }

        private string CreateSampleCsvFile()
        {
            var headers = new List<string> { "City" };
            var rows = new List<List<string>>
            {
                new() { "New York" },
                new() { "Los Angeles" },
                new() { "Chicago" }
            };

            return CreateTempCsvFile("sample_cities.csv", headers, rows);
        }

        private string CreateEmptyCsvFile()
        {
            var headers = new List<string> { "City" };
            var rows = new List<List<string>>(); // No rows

            return CreateTempCsvFile("empty.csv", headers, rows);
        }

        #endregion
        #endregion
    }
}
