using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Phonetics;
using MatchLogic.Domain.Entities;
using MatchLogic.Infrastructure.Comparator;
using MatchLogic.Infrastructure.Phonetics;
using FluentValidation.Validators;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Phonetics;
using MatchLogic.Domain.Entities;
using MatchLogic.Infrastructure.Comparator;
using MatchLogic.Infrastructure.Phonetics;
using NPOI.SS.Formula.Functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;

public class ComparatorTests
{
    private readonly IStringSimilarityCalculator _similarityCalculator;
    private readonly ITransliterator _transliterator;
    private readonly IPhoneticEncoder _phoneticEncoder;
    private readonly PhoneticConverter _phoneticConverter;
    private IComparator _comparator;
    private readonly ComparatorBuilder _builder;

    public ComparatorTests()
    {
        // Use actual JaroWinkler Calculator
        _similarityCalculator = new JaroWinklerCalculator();

        //Use actual Unidecode Transliterator and Phonix Encoder
        _transliterator = new UnidecodeTransliterator();
        _phoneticEncoder = new PhonixEncoder();

        //Use actual PhoneticConverter
        _phoneticConverter = new PhoneticConverter(_transliterator, _phoneticEncoder);

        // Create factories
        var configFactory = new ComparatorConfigFactory();
        var strategyFactory = new ComparatorStrategyFactory(_similarityCalculator, _phoneticConverter);

        // Create builder
        _builder = new ComparatorBuilder(configFactory, strategyFactory);
    }

    // String Comparison Tests
    [Theory]
    [InlineData("MARTHA", "MARHTA", 0.8, 0.961)]    // Classic JaroWinkler case, above threshold
    [InlineData("DIXON", "DICKSONX", 0.8, 0.813)]   // Above threshold
    [InlineData("MARTHA", "MARHTA", 0.97, 0)]       // Below threshold
    [InlineData("same", "same", 0.8, 1.0)]          // Exact match
    public void StringComparison_WithVariousInputs_ReturnsExpectedResults(
        string input1, string input2, double threshold, double expectedScore)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.Level, threshold.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedScore, result, 3); // Using precision of 3 decimal places
    }

    [Theory]
    [InlineData("color", "colour", 0.8)]          // British vs American spelling
    [InlineData("Michelle", "Michele", 0.8)]       // Common name variations
    [InlineData("John", "Jon", 0.8)]              // Name variations
    [InlineData("Christopher", "Kristopher", 0.8)] // Different first letter but similar
    public void StringComparison_WithCommonVariations_ReturnsHighScore(
        string input1, string input2, double threshold)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.Level, threshold.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.True(result > 0);
    }

    [Theory]
    [InlineData("", "test")]
    [InlineData("test", "")]
    [InlineData(null, "test")]
    [InlineData("test", null)]
    public void StringComparison_WithInvalidInputs_ReturnsZero(string input1, string input2)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.Level, 0.9.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(0, result);
    }

    // Numeric Comparison Tests
    [Theory]
    [InlineData("100", "105", 10, 10, 0.75)]    // Within range
    [InlineData("100", "100", 10, 10, 1.0)]    // Exact match
    [InlineData("100", "110", 10, 10, 0.5)]      // At upper bound
    [InlineData("100", "90", 10, 10, 0.5)]       // At lower bound
    [InlineData("100", "120", 10, 10, 0)]      // Outside upper bound
    [InlineData("100", "80", 10, 10, 0)]       // Outside lower bound
    public void NumericComparison_WithVariousInputs_ReturnsExpectedResults(
        string input1, string input2, decimal upperLimit, decimal lowerLimit, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.UpperLimit, upperLimit.ToString() },
            { ArgsValue.LowerLimit, lowerLimit.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 3);
    }

    [Theory]
    [InlineData("abc", "100")]
    [InlineData("100", "abc")]
    [InlineData("", "100")]
    [InlineData("100", "")]
    [InlineData(null, "100")]
    [InlineData("100", null)]
    public void NumericComparison_WithInvalidInputs_ReturnsZero(string input1, string input2)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.UpperLimit, 10m.ToString() },
            { ArgsValue.LowerLimit, 10m.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(0, result);
    }

    [Theory]
    [InlineData("100", "105", 5, 10)]   // Asymmetric range
    [InlineData("100", "95", 5, 10)]    // Asymmetric range
    public void NumericComparison_WithAsymmetricRanges_HandlesCorrectly(
        string input1, string input2, decimal upperLimit, decimal lowerLimit)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.UpperLimit, upperLimit.ToString() },
            { ArgsValue.LowerLimit, lowerLimit.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.InRange(result, 0, 1);
    }

    [Theory]
    [InlineData("MARTHA", "MARTHA", 0.8)]     // Same case
    [InlineData("martha", "MARTHA", 0.8)]     // Different case
    [InlineData("Martha", "martha", 0.8)]     // Mixed case
    public void StringComparison_CaseInsensitivity_ReturnsConsistentResults(
        string input1, string input2, double threshold)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.Level, threshold.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(1.0, result);
    }

    [Theory]
    [InlineData("New York", "NewYork", 0.8)]          // Space difference
    [InlineData("O'Connor", "OConnor", 0.8)]          // Apostrophe
    [InlineData("McDonald", "MacDonald", 0.8)]        // Name variants
    public void StringComparison_WithSpecialCases_HandlesAppropriately(
        string input1, string input2, double threshold)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.Level, threshold.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.True(result > 0);
    }

    [Theory]
    [InlineData("100.00", "100", 10, 10, 1.0)]       // Different decimal places
    [InlineData("1000.50", "1000.5", 10, 10, 1.0)]   // Different decimal representation
    [InlineData("-100", "-100.00", 10, 10, 1.0)]     // Negative numbers
    public void NumericComparison_WithDifferentFormats_HandlesCorrectly(
        string input1, string input2, decimal upperLimit, decimal lowerLimit, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.UpperLimit, upperLimit.ToString() },
            { ArgsValue.LowerLimit, lowerLimit.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [InlineData("100", "100.75", 0.75, 0.75, 0.5)]        // Within range ±0.75
    [InlineData("100", "100.25", 0.75, 0.75, 0.83333333333333337)]        // Within range ±0.75
    [InlineData("100", "99.25", 0.75, 0.75, 0.5)]         // Within range ±0.75
    [InlineData("100", "99.75", 0.75, 0.75, 0.83333333333333337)]         // Within range ±0.75
    [InlineData("100", "100.76", 0.75, 0.75, 0)]        // Just outside range ±0.75
    [InlineData("100", "99.24", 0.75, 0.75, 0)]         // Just outside range ±0.75
    public void NumericComparison_WithDecimalLimits_HandlesCorrectly(
    string input1, string input2, decimal upperLimit, decimal lowerLimit, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperLimit, upperLimit.ToString() },
        { ArgsValue.LowerLimit, lowerLimit.ToString() }
    };
        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [InlineData("10.5", "10.7", 0.25, 0.25, 0.6)]       // Within small decimal range
    [InlineData("10.5", "10.6", 0.25, 0.25, 0.8)]       // Within small decimal range
    [InlineData("10.5", "10.75", 0.25, 0.25, 0.5)]        // At upper limit
    [InlineData("10.5", "10.25", 0.25, 0.25, 0.5)]        // Within lower range
    [InlineData("10.5", "10.24", 0.25, 0.25, 0)]        // Just outside lower range
    public void NumericComparison_WithSmallDecimalLimits_CalculatesScoreCorrectly(
        string input1, string input2, decimal upperLimit, decimal lowerLimit, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
        {
            { ArgsValue.UpperLimit, upperLimit.ToString() },
            { ArgsValue.LowerLimit, lowerLimit.ToString() }
        };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 1); // Precision of 1 decimal place
    }

    [Theory]
    [InlineData("100.55", "100.60", 0.1, 0.05, 0.67)]         // Asymmetric decimal limits
    [InlineData("100.55", "100.50", 0.1, 0.05, 0.67)]         // Asymmetric decimal limits
    [InlineData("100.55", "100.66", 0.1, 0.05, 0)]      // Outside asymmetric upper limit
    [InlineData("100.55", "100.49", 0.1, 0.05, 0)]      // Outside asymmetric lower limit
    public void NumericComparison_WithAsymmetricDecimalLimits_HandlesCorrectly(
        string input1, string input2, decimal upperLimit, decimal lowerLimit, double expectedResult = 1)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperLimit, upperLimit.ToString() },
        { ArgsValue.LowerLimit, lowerLimit.ToString() }
    };
        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, Math.Round(result, 2));
    }

    [Theory]
    [InlineData("0.001", "0.00125", 0.0005, 0.0005, 0.75)]     // Very small decimal numbers
    [InlineData("0.001", "0.00151", 0.0005, 0.0005, 0)]     // Outside very small range
    [InlineData("0.001", "0.00075", 0.0005, 0.0005, 0.75)]     // Within very small range
    [InlineData("0.001", "0.00049", 0.0005, 0.0005, 0)]     // Outside very small range
    public void NumericComparison_WithVerySmallDecimalLimits_HandlesCorrectly(
        string input1, string input2, decimal upperLimit, decimal lowerLimit, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperLimit, upperLimit.ToString() },
        { ArgsValue.LowerLimit, lowerLimit.ToString() }
    };
        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [InlineData("999999.99", "1000000.24", 0.25, 0.25, 0.5)]  // Large numbers with small decimal limits
    [InlineData("999999.99", "999999.74", 0.25, 0.25, 0.5)]   // Within range
    [InlineData("-999999.99", "-999999.74", 0.25, 0.25, 0.5)] // Negative large numbers
    [InlineData("-999999.99", "-1000000.24", 0.25, 0.25, 0.5)] // Outside range
    public void NumericComparison_LargeNumbersWithDecimalLimits_HandlesCorrectly(
        string input1, string input2, decimal upperLimit, decimal lowerLimit, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperLimit, upperLimit.ToString() },
        { ArgsValue.LowerLimit, lowerLimit.ToString() }
    };
        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [InlineData("1.5", "1.75", 0.25, 0, 0)]                    // Only upper decimal limit
    [InlineData("1.5", "1.25", 0, 0.25, 0)]                    // Only lower decimal limit
    [InlineData("1.5", "1.76", 0.25, 0, 0)]                 // Outside upper decimal limit
    [InlineData("1.5", "1.24", 0, 0.25, 0)]                 // Outside lower decimal limit
    public void NumericComparison_WithSingleDecimalLimit_HandlesCorrectly(
        string input1, string input2, decimal upperLimit, decimal lowerLimit, double expectedResult = 1)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperLimit, upperLimit.ToString() },
        { ArgsValue.LowerLimit, lowerLimit.ToString() }
    };
        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    // Percentage Numeric Comparison Tests
    [Theory]
    [InlineData("100", "105", 10, 10, 0.75)]    // 5% above, within 10% upper bound
    [InlineData("100", "100", 10, 10, 1.0)]     // Exact match
    [InlineData("100", "110", 10, 10, 0.5)]     // At 10% upper bound
    [InlineData("100", "90", 10, 10, 0.5)]      // At 10% lower bound
    [InlineData("100", "120", 10, 10, 0)]       // 20% above, outside bound
    [InlineData("100", "80", 10, 10, 0)]        // 20% below, outside bound
    [InlineData("100", "95", 10, 10, 0.75)]     // 5% below, within bound
    public void PercentageNumericComparison_WithVariousInputs_ReturnsExpectedResults(
    string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 3);
    }

    [Theory]
    [InlineData("1000", "1050", 5, 5, 0.5)]      // At 5% upper bound
    [InlineData("1000", "1025", 5, 5, 0.75)]     // 2.5% above, within 5% bound
    [InlineData("1000", "950", 5, 5, 0.5)]       // At 5% lower bound
    [InlineData("1000", "975", 5, 5, 0.75)]      // 2.5% below, within 5% bound
    [InlineData("1000", "1060", 5, 5, 0)]        // 6% above, outside bound
    [InlineData("1000", "940", 5, 5, 0)]         // 6% below, outside bound
    public void PercentageNumericComparison_WithSmallPercentages_HandlesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 3);
    }

    [Theory]
    [InlineData("100", "105", 5, 10, 0.67)]      // Asymmetric: 5% up, 10% down
    [InlineData("100", "95", 5, 10, 0.67)]       // Within 10% lower bound
    [InlineData("100", "90", 5, 10, 0.33)]       // At 10% lower bound
    [InlineData("100", "106", 5, 10, 0)]         // Outside 5% upper bound
    [InlineData("100", "89", 5, 10, 0)]          // Outside 10% lower bound
    public void PercentageNumericComparison_WithAsymmetricPercentages_HandlesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, Math.Round(result, 2));
    }

    [Theory]
    [InlineData("10", "11", 10, 10, 0.5)]        // Small numbers
    [InlineData("10", "9", 10, 10, 0.5)]
    [InlineData("1", "1.1", 10, 10, 0.5)]        // Very small numbers
    [InlineData("1", "0.9", 10, 10, 0.5)]
    [InlineData("0.1", "0.11", 10, 10, 0.5)]     // Decimal numbers
    [InlineData("0.1", "0.09", 10, 10, 0.5)]
    public void PercentageNumericComparison_WithSmallNumbers_CalculatesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 2);
    }

    [Theory]
    [InlineData("1000000", "1100000", 10, 10, 0.5)]     // Large numbers at 10%
    [InlineData("1000000", "1050000", 10, 10, 0.75)]    // Large numbers at 5%
    [InlineData("1000000", "900000", 10, 10, 0.5)]      // Large numbers at -10%
    [InlineData("1000000", "950000", 10, 10, 0.75)]     // Large numbers at -5%
    public void PercentageNumericComparison_WithLargeNumbers_HandlesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 3);
    }

    [Theory]
    [InlineData("-100", "-105", 10, 10, 0.75)]    // Negative numbers
    [InlineData("-100", "-95", 10, 10, 0.75)]
    [InlineData("-100", "-110", 10, 10, 0.5)]
    [InlineData("-100", "-90", 10, 10, 0.5)]
    [InlineData("-100", "-120", 10, 10, 0)]
    [InlineData("-100", "-80", 10, 10, 0)]
    public void PercentageNumericComparison_WithNegativeNumbers_HandlesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 3);
    }

    [Theory]
    [InlineData("0", "0", 10, 10, 1.0)]          // Zero exact match
    [InlineData("0", "1", 10, 10, 0)]            // Zero with non-zero
    [InlineData("0", "-1", 10, 10, 0)]           // Zero with negative
    [InlineData("0", "0.1", 10, 10, 0)]          // Zero with small decimal
    public void PercentageNumericComparison_WithZeroValue_HandlesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [InlineData("abc", "100")]
    [InlineData("100", "abc")]
    [InlineData("", "100")]
    [InlineData("100", "")]
    [InlineData(null, "100")]
    [InlineData("100", null)]
    public void PercentageNumericComparison_WithInvalidInputs_ReturnsZero(string input1, string input2)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, "10" },
        { ArgsValue.LowerPercentage, "10" }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(0, result);
    }

    [Theory]
    [InlineData("100.00", "110.00", 10, 10, 0.5)]       // Different decimal places
    [InlineData("100.50", "110.55", 10, 10, 0.5)]       // Decimal with percentages
    [InlineData("100", "110.0", 10, 10, 0.5)]           // Mixed formats
    public void PercentageNumericComparison_WithDifferentFormats_HandlesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 2);
    }

    [Theory]
    [InlineData("100", "101", 1, 1, 0.5)]        // 1% tolerance
    [InlineData("100", "100.5", 1, 1, 0.75)]     // 0.5% difference
    [InlineData("100", "99", 1, 1, 0.5)]         // 1% below
    [InlineData("100", "99.5", 1, 1, 0.75)]      // 0.5% below
    [InlineData("100", "102", 1, 1, 0)]          // Outside 1% tolerance
    [InlineData("100", "98", 1, 1, 0)]           // Outside 1% tolerance
    public void PercentageNumericComparison_WithVerySmallPercentages_HandlesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 3);
    }

    [Theory]
    [InlineData("100", "150", 50, 50, 0.5)]      // 50% tolerance
    [InlineData("100", "125", 50, 50, 0.75)]     // 25% above
    [InlineData("100", "50", 50, 50, 0.5)]       // 50% below
    [InlineData("100", "75", 50, 50, 0.75)]      // 25% below
    [InlineData("100", "200", 50, 50, 0)]        // 100% above, outside bound
    [InlineData("100", "0", 50, 50, 0)]          // 100% below, outside bound
    public void PercentageNumericComparison_WithLargePercentages_HandlesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 3);
    }

    [Theory]
    [InlineData("100", "100", 0, 0, 1.0)]        // Zero percentages - exact match only
    [InlineData("100", "100.01", 0, 0, 0)]       // Zero percentages - no tolerance
    [InlineData("100", "99.99", 0, 0, 0)]        // Zero percentages - no tolerance
    public void PercentageNumericComparison_WithZeroPercentages_RequiresExactMatch(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [InlineData("50.5", "52.525", 5, 5, 0.6)]     // 5% of 50.5 = 2.525 upper bound
    [InlineData("50.5", "51.5125", 5, 5, 0.8)]   // 2.5% above
    [InlineData("50.5", "48.475", 5, 5, 0.6)]     // 5% of 50.5 = 2.525 lower bound
    [InlineData("50.5", "49.4875", 5, 5, 0.8)]   // 2.5% below
    public void PercentageNumericComparison_WithDecimalBaseValues_CalculatesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 2);
    }

    [Theory]
    [InlineData("1000", "1200", 20, 10, 0.33)]    // +20% at upper bound
    [InlineData("1000", "1100", 20, 10, 0.67)]   // +10% within upper bound
    [InlineData("1000", "900", 20, 10, 0.67)]     // -10% at lower bound
    [InlineData("1000", "950", 20, 10, 0.83)]    // -5% within lower bound
    [InlineData("1000", "1250", 20, 10, 0)]      // +25% outside upper bound
    [InlineData("1000", "850", 20, 10, 0)]       // -15% outside lower bound
    public void PercentageNumericComparison_WithDifferentUpperLowerPercentages_HandlesCorrectly(
        string input1, string input2, decimal upperPercentage, decimal lowerPercentage, double expectedResult)
    {
        // Arrange
        var args = new Dictionary<ArgsValue, string>
    {
        { ArgsValue.UpperPercentage, upperPercentage.ToString() },
        { ArgsValue.LowerPercentage, lowerPercentage.ToString() }
    };

        _comparator = _builder.WithArgs(args).Build();

        // Act
        var result = _comparator.Compare(input1, input2);

        // Assert
        Assert.Equal(expectedResult, result, 3);
    }
}
