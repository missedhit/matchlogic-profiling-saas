using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Interfaces.Phonetics;
using MatchLogic.Infrastructure.Phonetics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class PhoneticConverterTests
{
    private readonly ITransliterator _transliterator;
    private readonly IPhoneticEncoder _phoneticEncoder;

    public PhoneticConverterTests()
    {
        _transliterator = new UnidecodeTransliterator();
        _phoneticEncoder = new PhonixEncoder();
    }

    [Fact]
    public void ConvertToPhonetic_WithTransliterator_ShouldTransliterateAndEncode()
    {
        // Arrange
        var converter = new PhoneticConverter(_transliterator, _phoneticEncoder);

        // Act
        var result = converter.ConvertToPhonetic("Привет");

        // Assert
        Assert.Equal("PRFT", result);
    }

    [Fact]
    public void ConvertToPhonetic_WithoutTransliterator_ShouldOnlyEncode()
    {
        // Arrange
        var converter = new PhoneticConverter(null, _phoneticEncoder);

        // Act
        var result = converter.ConvertToPhonetic("Привет");

        // Assert
        Assert.NotEqual("PRFT", result);
    }

    [Fact]
    public void ConvertToPhonetic_WithEmptyInput_ShouldReturnEmptyString()
    {
        // Arrange
        var converter = new PhoneticConverter(_transliterator, _phoneticEncoder);

        // Act
        var result = converter.ConvertToPhonetic(string.Empty);

        // Assert
        Assert.Equal(string.Empty, result);
    }

    [Fact]
    public void ConvertToPhonetic_WithNullInput_ShouldThrowArgumentNullException()
    {
        // Arrange
        var converter = new PhoneticConverter(_transliterator, _phoneticEncoder);

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => converter.ConvertToPhonetic(null));
    }

    [Fact]
    public void Constructor_WithNullEncoder_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new PhoneticConverter(_transliterator, null));
    }

    [Theory]
    [InlineData("cate", "KT")]
    [InlineData("kat", "KT")]
    [InlineData("Owais", "A")]
    [InlineData("Ovais", "AF")]
    [InlineData("Awais", "A")]
    [InlineData("Привет", "PRFT")]
    [InlineData("こんにちは", "KNXH")]
    public void ConvertToPhonetic_WithDifferentInputs_ShouldWorkCorrectly(string input, string expected)
    {
        // Arrange
        var converter = new PhoneticConverter(_transliterator, _phoneticEncoder);

        // Act
        var result = converter.ConvertToPhonetic(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ConvertToPhonetic_WithAndWithoutTransliteration_ShouldProduceSameResultForASCII()
    {
        // Arrange
        var converterWithTransliteration = new PhoneticConverter(_transliterator, _phoneticEncoder);
        var converterWithoutTransliteration = new PhoneticConverter(null, _phoneticEncoder);
        var input = "Hello World";

        // Act
        var resultWithTransliteration = converterWithTransliteration.ConvertToPhonetic(input);
        var resultWithoutTransliteration = converterWithoutTransliteration.ConvertToPhonetic(input);

        // Assert
        Assert.Equal(resultWithTransliteration, resultWithoutTransliteration);
    }

    [Fact]
    public void ConvertToPhonetic_NonASCIIInput_ShouldDifferWithAndWithoutTransliteration()
    {
        // Arrange
        var converterWithTransliteration = new PhoneticConverter(_transliterator, _phoneticEncoder);
        var converterWithoutTransliteration = new PhoneticConverter(null, _phoneticEncoder);
        var input = "Привет";//"Müller";

        // Act
        var resultWithTransliteration = converterWithTransliteration.ConvertToPhonetic(input);
        var resultWithoutTransliteration = converterWithoutTransliteration.ConvertToPhonetic(input);

        // Assert
        Assert.NotEqual(resultWithTransliteration, resultWithoutTransliteration);
    }
}
