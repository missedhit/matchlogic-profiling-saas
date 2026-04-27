using MatchLogic.Application.Common;
using MatchLogic.Application.Core;
using MatchLogic.Domain.Regex;
using System;
using System.Collections.Generic;

namespace MatchLogic.Application.Core;

public class RegexSeedProvider : DataSeedProviderBase<RegexInfo>
{
    /// <summary>
    /// Gets the collection name for regex patterns
    /// </summary>
    public override string GetCollectionName() => Constants.Collections.RegexInfo;

    /// <summary>
    /// Gets the seed data for common regex patterns
    /// </summary>
    public override IEnumerable<RegexInfo> GetSeedData()
    {
        var regexPatterns = new List<RegexInfo>
        {
            // Basic data types
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "Email Address",
                Description = "Matches valid email addresses",
                RegexExpression = @"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            },
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "Phone Number (US)",
                Description = "Matches US phone numbers in various formats",
                RegexExpression = @"^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            },
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "URL",
                Description = "Matches URLs (http, https)",
                RegexExpression = @"^(https?:\/\/)?(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            },
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "ZIP Code (US)",
                Description = "Matches US ZIP codes (5 or 9 digits)",
                RegexExpression = @"^\d{5}(-\d{4})?$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            },
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "IP Address (IPv4)",
                Description = "Matches IPv4 addresses",
                RegexExpression = @"^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            },
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "Date (YYYY-MM-DD)",
                Description = "Matches date in YYYY-MM-DD format",
                RegexExpression = @"^\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            },
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "Credit Card Number",
                Description = "Matches credit card numbers (spaces or dashes optional)",
                RegexExpression = @"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            },
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "Numeric",
                Description = "Matches pure numeric values",
                RegexExpression = @"^[0-9]+$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            },
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "Decimal Number",
                Description = "Matches decimal numbers",
                RegexExpression = @"^-?\d+(\.\d+)?$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            },
            new RegexInfo
            {
                Id = Guid.NewGuid(),
                Name = "Alpha Only",
                Description = "Matches only alphabetic characters",
                RegexExpression = @"^[A-Za-z]+$",
                IsDefault = true,
                IsSystem = true,
                IsSystemDefault = true,
                IsDeleted = false,
                Version = 1
            }
        };

        return regexPatterns;
    }
}
