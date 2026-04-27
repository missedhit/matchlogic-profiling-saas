using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Regex;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Regex;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests
{
    public class RegexInfoServiceTests
    {
        private readonly Mock<IGenericRepository<RegexInfo, Guid>> _mockRepository;
        private readonly Mock<ILogger<RegexInfoService>> _mockLogger;
        private readonly RegexInfoService _service;

        public RegexInfoServiceTests()
        {
            _mockRepository = new Mock<IGenericRepository<RegexInfo, Guid>>();
            _mockLogger = new Mock<ILogger<RegexInfoService>>();
            _service = new RegexInfoService(_mockRepository.Object, _mockLogger.Object);
        }

        #region GetAllRegexInfo Tests

        [Fact]
        public async Task GetAllRegexInfo_ShouldReturnAllRegexInfos()
        {
            // Arrange
            var expectedRegexInfos = new List<RegexInfo>
            {
                new RegexInfo { Id = Guid.NewGuid(), Name = "Test1" },
                new RegexInfo { Id = Guid.NewGuid(), Name = "Test2" }
            };

            _ = _mockRepository
                .Setup(repo => repo.GetAllAsync(Constants.Collections.RegexInfo))
                .ReturnsAsync(expectedRegexInfos);

            // Act
            var result = await _service.GetAllRegexInfo();

            // Assert
            Assert.Equal(expectedRegexInfos.Count, result.Count);
            Assert.Equal(expectedRegexInfos, result);
            _mockRepository.Verify(repo => repo.GetAllAsync(Constants.Collections.RegexInfo), Times.Once);
        }

        [Fact]
        public async Task GetAllRegexInfo_WhenNoRegexInfosExist_ShouldReturnEmptyList()
        {
            // Arrange
            _ = _mockRepository
                .Setup(repo => repo.GetAllAsync(Constants.Collections.RegexInfo))
                .ReturnsAsync(new List<RegexInfo>());

            // Act
            var result = await _service.GetAllRegexInfo();

            // Assert
            Assert.Empty(result);
            _mockRepository.Verify(repo => repo.GetAllAsync(Constants.Collections.RegexInfo), Times.Once);
        }

        #endregion

        #region GetRegexInfoById Tests

        [Fact]
        public async Task GetRegexInfoById_WithValidId_ShouldReturnRegexInfo()
        {
            // Arrange
            var id = Guid.NewGuid();
            var expectedRegexInfo = new RegexInfo { Id = id, Name = "Test" };

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                .ReturnsAsync(expectedRegexInfo);

            // Act
            var result = await _service.GetRegexInfoById(id);

            // Assert
            Assert.Equal(expectedRegexInfo, result);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
        }

        [Fact]
        public async Task GetRegexInfoById_WithInvalidId_ShouldReturnNull()
        {
            // Arrange
            var id = Guid.NewGuid();

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                ?.ReturnsAsync((RegexInfo)null);

            // Act
            var result = await _service.GetRegexInfoById(id);

            // Assert
            Assert.Null(result);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
        }

        #endregion

        #region CreateRegexInfo Tests

        [Fact]
        public async Task CreateRegexInfo_ShouldCreateAndReturnNewRegexInfo()
        {
            // Arrange
            var name = "Test Regex";
            var description = "Test Description";
            var regexExpression = "^[a-z]+$";
            var isDefault = true;

            _mockRepository
                .Setup(repo => repo.InsertAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo));
                

            // Act
            var result = await _service.CreateRegexInfo(name, description, regexExpression, isDefault);

            // Assert
            Assert.Equal(name, result.Name);
            Assert.Equal(description, result.Description);
            Assert.Equal(regexExpression, result.RegexExpression);
            Assert.Equal(isDefault, result.IsDefault);
            Assert.False(result.IsSystem);
            Assert.False(result.IsSystemDefault);
            Assert.False(result.IsDeleted);
            Assert.Equal(-1, result.Version);

            _mockRepository.Verify(repo => repo.InsertAsync(
                It.Is<RegexInfo>(r =>
                    r.Name == name &&
                    r.Description == description &&
                    r.RegexExpression == regexExpression &&
                    r.IsDefault == isDefault),
                Constants.Collections.RegexInfo),
                Times.Once);
        }

        #endregion

        #region UpdateRegexInfo Tests

        [Fact]
        public async Task UpdateRegexInfo_WithValidRegexInfo_ShouldUpdateRegexInfo()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingRegexInfo = new RegexInfo
            {
                Id = id,
                Name = "Old Name",
                Description = "Old Description",
                RegexExpression = "Old Expression",
                IsDefault = false,
                IsSystem = false
            };

            var updatedRegexInfo = new RegexInfo
            {
                Id = id,
                Name = "New Name",
                Description = "New Description",
                RegexExpression = "New Expression",
                IsDefault = true
            };

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                .ReturnsAsync(existingRegexInfo);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo));
            
            // Act
            await _service.UpdateRegexInfo(updatedRegexInfo);

            // Assert
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(
                It.Is<RegexInfo>(r =>
                    r.Id == id &&
                    r.Name == updatedRegexInfo.Name &&
                    r.Description == updatedRegexInfo.Description &&
                    r.RegexExpression == updatedRegexInfo.RegexExpression &&
                    r.IsDefault == updatedRegexInfo.IsDefault),
                Constants.Collections.RegexInfo),
                Times.Once);
        }

        [Fact]
        public async Task UpdateRegexInfo_WithNonExistentRegexInfo_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();
            var updatedRegexInfo = new RegexInfo
            {
                Id = id,
                Name = "New Name",
                Description = "New Description",
                RegexExpression = "New Expression",
                IsDefault = true
            };

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                .ReturnsAsync((RegexInfo)null);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.UpdateRegexInfo(updatedRegexInfo));
            Assert.Equal("RegexInfo not found", exception.Message);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo), Times.Never);
        }

        [Fact]
        public async Task UpdateRegexInfo_WithSystemRegexInfo_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingRegexInfo = new RegexInfo
            {
                Id = id,
                Name = "System Regex",
                IsSystem = true
            };

            var updatedRegexInfo = new RegexInfo
            {
                Id = id,
                Name = "New Name"
            };

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                .ReturnsAsync(existingRegexInfo);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.UpdateRegexInfo(updatedRegexInfo));
            Assert.Equal("Cannot update system regex", exception.Message);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo), Times.Never);
        }

        #endregion

        #region DeleteRegexInfo Tests

        [Fact]
        public async Task DeleteRegexInfo_WithValidId_ShouldMarkAsDeleted()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingRegexInfo = new RegexInfo
            {
                Id = id,
                Name = "Test Regex",
                IsDeleted = false,
                IsSystem = false
            };

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                .ReturnsAsync(existingRegexInfo);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo));

            // Act
            await _service.DeleteRegexInfo(id);

            // Assert
            Assert.True(existingRegexInfo.IsDeleted);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(
                It.Is<RegexInfo>(r => r.Id == id && r.IsDeleted),
                Constants.Collections.RegexInfo),
                Times.Once);
        }

        [Fact]
        public async Task DeleteRegexInfo_WithNonExistentId_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                .ReturnsAsync((RegexInfo)null);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.DeleteRegexInfo(id));
            Assert.Equal("RegexInfo not found", exception.Message);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo), Times.Never);
        }

        [Fact]
        public async Task DeleteRegexInfo_WithSystemRegexInfo_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingRegexInfo = new RegexInfo
            {
                Id = id,
                Name = "System Regex",
                IsSystem = true
            };

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                .ReturnsAsync(existingRegexInfo);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.DeleteRegexInfo(id));
            Assert.Equal("Cannot delete system regex", exception.Message);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo), Times.Never);
        }

        #endregion

        #region SetDefault Tests

        [Fact]
        public async Task SetDefault_WithValidId_ShouldSetAsDefault()
        {
            // Arrange
            var id = Guid.NewGuid();
            var existingRegexInfo = new RegexInfo
            {
                Id = id,
                Name = "Test Regex",
                IsDefault = false
            };

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                .ReturnsAsync(existingRegexInfo);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo));

            // Act
            await _service.SetDefault(id, true);

            // Assert
            Assert.True(existingRegexInfo.IsDefault);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(
                It.Is<RegexInfo>(r => r.Id == id && r.IsDefault),
                Constants.Collections.RegexInfo),
                Times.Once);
        }

        [Fact]
        public async Task SetDefault_WithNonExistentId_ShouldThrowException()
        {
            // Arrange
            var id = Guid.NewGuid();

            _ = _mockRepository
                .Setup(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo))
                .ReturnsAsync((RegexInfo)null);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => _service.SetDefault(id, true));
            Assert.Equal("RegexInfo not found", exception.Message);
            _mockRepository.Verify(repo => repo.GetByIdAsync(id, Constants.Collections.RegexInfo), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo), Times.Never);
        }

        #endregion

        #region ResetSystemDefaults Tests

        [Fact]
        public async Task ResetSystemDefaults_ShouldResetDefaultFlags()
        {
            // Arrange
            var regexInfos = new List<RegexInfo>
            {
                new RegexInfo { Id = Guid.NewGuid(), Name = "System Default 1", IsSystemDefault = true, IsDefault = false },
                new RegexInfo { Id = Guid.NewGuid(), Name = "System Default 2", IsSystemDefault = true, IsDefault = false },
                new RegexInfo { Id = Guid.NewGuid(), Name = "Non System Default", IsSystemDefault = false, IsDefault = true }
            };

            _ = _mockRepository
                .Setup(repo => repo.GetAllAsync(Constants.Collections.RegexInfo))
                .ReturnsAsync(regexInfos);

            _mockRepository
                .Setup(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo));

            // Act
            await _service.ResetSystemDefaults();

            // Assert
            Assert.True(regexInfos[0].IsDefault);
            Assert.True(regexInfos[1].IsDefault);
            Assert.False(regexInfos[2].IsDefault);

            _mockRepository.Verify(repo => repo.GetAllAsync(Constants.Collections.RegexInfo), Times.Once);
            _mockRepository.Verify(repo => repo.UpdateAsync(It.IsAny<RegexInfo>(), Constants.Collections.RegexInfo), Times.Exactly(3));
        }

        #endregion
    }
}
