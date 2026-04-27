using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Infrastructure.Security;
using ExcelDataReader.Log;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using static Org.BouncyCastle.Math.EC.ECCurve;

namespace MatchLogic.Application.UnitTests
{
    public class SecureParametersHandlerTest
    {
        ILogger<EncryptionService> encryptLogger = new NullLogger<EncryptionService>();
        ILogger<SecureParameterHandler> secureLogger = new NullLogger<SecureParameterHandler>();
        public SecureParametersHandlerTest()
        {
        }

        [Fact]
        public void ValidParameters_ShouldEncryptAndDecrypt()
        {
            // Arrange
            var parameters = new Dictionary<string, string>
            {
                { "Username", "testuser" },
                { "Password", "P@ssw0rd!" },
                { "Server", "localhost" }
            };
            var dataSourceId = Guid.NewGuid();
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    { "Security:MasterKey", "TestMasterKey123456789012345678901234" }
                })
                .Build();
            var secureParameterHandler = new SecureParameterHandler(new EncryptionService(config, encryptLogger), secureLogger);
            // Act
            var encryptedParamsTask = secureParameterHandler.EncryptSensitiveParametersAsync(parameters, dataSourceId);
            encryptedParamsTask.Wait();
            var encryptedParams = encryptedParamsTask.Result;
            var decryptedParamsTask = secureParameterHandler.DecryptSensitiveParametersAsync(encryptedParams, dataSourceId);
            decryptedParamsTask.Wait();
            var decryptedParams = decryptedParamsTask.Result;
            // Assert
            Assert.Equal(parameters["Username"], decryptedParams["Username"]);
            Assert.Equal(parameters["Password"], decryptedParams["Password"]);
            Assert.Equal(parameters["Server"], decryptedParams["Server"]);
        }
        [Fact]
        public void EmptyParameters_ShouldReturnEmpty()
        {
            // Arrange
            var parameters = new Dictionary<string, string>();
            var dataSourceId = Guid.NewGuid();
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    { "Security:MasterKey", "TestMasterKey123456789012345678901234" }
                })
                .Build();
            var secureParameterHandler = new SecureParameterHandler(new EncryptionService(config, encryptLogger), secureLogger);
            // Act
            var encryptedParamsTask = secureParameterHandler.EncryptSensitiveParametersAsync(parameters, dataSourceId);
            encryptedParamsTask.Wait();
            var encryptedParams = encryptedParamsTask.Result;
            var decryptedParamsTask = secureParameterHandler.DecryptSensitiveParametersAsync(encryptedParams, dataSourceId);
            decryptedParamsTask.Wait();
            var decryptedParams = decryptedParamsTask.Result;
            // Assert
            Assert.Empty(encryptedParams);
            Assert.Empty(decryptedParams);
        }
        [Fact]
        public void NullParameters_ShouldReturnNull()
        {
            // Arrange
            Dictionary<string, string> parameters = null;
            var dataSourceId = Guid.NewGuid();
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    { "Security:MasterKey", "TestMasterKey123456789012345678901234" }
                })
                .Build();
            var secureParameterHandler = new SecureParameterHandler(new EncryptionService(config, encryptLogger), secureLogger);
            // Act
            var encryptedParamsTask = secureParameterHandler.EncryptSensitiveParametersAsync(parameters, dataSourceId);
            encryptedParamsTask.Wait();
            var encryptedParams = encryptedParamsTask.Result;
            var decryptedParamsTask = secureParameterHandler.DecryptSensitiveParametersAsync(encryptedParams, dataSourceId);
            decryptedParamsTask.Wait();
            var decryptedParams = decryptedParamsTask.Result;
            // Assert
            Assert.Null(encryptedParams);
            Assert.Null(decryptedParams);
        }
        [Theory]
        //[InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        public async Task EmptyMasterKey_ShouldThrow(string masterKey)
        {
            // Arrange
            var parameters = new Dictionary<string, string>
            {
                { "Username", "testuser" },
                { "Password", "P@ssw0rd!" }
            };
            var dataSourceId = Guid.NewGuid();
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    { "Security:MasterKey",masterKey } // Empty Master Key
                })
                .Build();
            // Act & Assert

            var result = Assert.Throws<System.InvalidOperationException>(() => new SecureParameterHandler(new EncryptionService(config, encryptLogger), secureLogger));
            //var result = await Assert.ThrowsAsync<System.InvalidOperationException>(() => secureParameterHandler.EncryptSensitiveParametersAsync(parameters, dataSourceId));
            Assert.Equal("Master key cannot be empty or null", result.Message);
        }
        //[Fact]
        //public async Task InvalidMasterKey_ShouldThrow()
        //{
        //    // Arrange
        //    var parameters = new Dictionary<string, string>
        //    {
        //        { "Username", "testuser" },
        //        { "Password", "P@ssw0rd!" }
        //    };
        //    var dataSourceId = Guid.NewGuid();
        //    var config = new ConfigurationBuilder()
        //        .AddInMemoryCollection(new Dictionary<string, string>
        //        {
        //            { "Security:MasterKey", "ShortKey" } // Invalid Master Key
        //        })
        //        .Build();
        //    var encryptLogger = new NullLogger<EncryptionService>();
        //    var secureLogger = new NullLogger<SecureParameterHandler>();
        //    // Act & Assert
        //    var result = Assert.Throws<System.InvalidOperationException>(() => new SecureParameterHandler(new EncryptionService(config, encryptLogger), secureLogger));
        //    Assert.Equal("Master key cannot be empty or null", result.Message);
        //}

        [Fact]
        public async Task MasterKeyNotConfigured_ShouldThrow()
        {
            // Arrange
            var parameters = new Dictionary<string, string>
            {
                { "Username", "testuser" },
                { "Password", "P@ssw0rd!" }
            };
            var dataSourceId = Guid.NewGuid();
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    
                })
                .Build();
            // Act & Assert
            var result = Assert.Throws<System.InvalidOperationException>(() => new SecureParameterHandler(new EncryptionService(config, encryptLogger), secureLogger));
            Assert.Equal("Master key not configured", result.Message);
        }
    }
}
