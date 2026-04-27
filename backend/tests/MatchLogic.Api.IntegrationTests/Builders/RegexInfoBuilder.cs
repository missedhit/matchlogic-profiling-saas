using DomainRegex = MatchLogic.Domain.Regex.RegexInfo;

using Microsoft.Extensions.DependencyInjection;
using MatchLogic.Application.Interfaces.Persistence;

namespace MatchLogic.Api.IntegrationTests.Builders;
public class RegexInfoBuilder
{

    private readonly DomainRegex _regex = new()
    {
        Name = "Test Regex",
        Description = "Test Regex Description",
        RegexExpression = "^[a-zA-Z0-9]+$",
        IsDefault = true,
        IsSystem = false,
        IsSystemDefault = false,
        IsDeleted = false,
    };
    private readonly IServiceProvider _serviceProvider;
    public RegexInfoBuilder(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    public RegexInfoBuilder WithValid()
    {
        _regex.Name = "Test Regex";
        _regex.Description = "Test Regex Description";
        //_regex.RegexExpression = "^[a-zA-Z0-9]+$";
        _regex.RegexExpression = RandomRegex();
        _regex.IsDefault = true;
        _regex.IsSystem = false;
        _regex.IsSystemDefault = false;
        _regex.IsDeleted = false;
        return this;
    }

    public RegexInfoBuilder WithId(Guid id)
    {
        _regex.Id = id;
        return this;
    }
    public RegexInfoBuilder WithName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Name cannot be null or empty.", nameof(name));
        _regex.Name = name;
        return this;
    }
    public RegexInfoBuilder WithDescription(string description)
    {
        if (string.IsNullOrWhiteSpace(description))
            throw new ArgumentException("Description cannot be null or empty.", nameof(description));
        _regex.Description = description;
        return this;
    }
    public RegexInfoBuilder WithRegexExpression(string regexExpression)
    {
        if (string.IsNullOrWhiteSpace(regexExpression))
            throw new ArgumentException("Regex expression cannot be null or empty.", nameof(regexExpression));
        _regex.RegexExpression = regexExpression;
        return this;
    }
    public RegexInfoBuilder WithIsDefault(bool isDefault)
    {
        _regex.IsDefault = isDefault;
        return this;
    }
    public RegexInfoBuilder WithIsSystem(bool isSystem)
    {
        _regex.IsSystem = isSystem;
        return this;
    }
    public RegexInfoBuilder WithIsSystemDefault(bool isSystemDefault)
    {
        _regex.IsSystemDefault = isSystemDefault;
        return this;
    }
    public RegexInfoBuilder WithIsDeleted(bool isDeleted)
    {
        _regex.IsDeleted = isDeleted;
        return this;
    }
    public DomainRegex BuildDomain() => _regex;
    public async Task<DomainRegex> BuildAsync()
    {
        var regexRepo = _serviceProvider.GetService<IGenericRepository<DomainRegex, Guid>>();
        await regexRepo.InsertAsync(_regex, Application.Common.Constants.Collections.RegexInfo);
        return _regex;
    }

    private string RandomRegex()
    {
        // Generate a unique random regex pattern
        var random = new Random();
        var length = random.Next(5, 15);
        var chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        var pattern = new char[length];
        for (int i = 0; i < length; i++)
        {
            // Randomly choose between a character or a character class
            if (random.NextDouble() < 0.7)
            {
                pattern[i] = chars[random.Next(chars.Length)];
            }
            else
            {
                // Add a simple character class
                pattern[i] = '.';
            }
        }
        var uniqueRegex = $"^{new string(pattern)}$";
        return uniqueRegex;
    }
}
