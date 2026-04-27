using MatchLogic.Application.Interfaces.Phonetics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching;
public class PhoneticConverter : IPhoneticConverter
{
    private readonly ITransliterator _transliterator;
    private readonly IPhoneticEncoder _phoneticEncoder;

    public PhoneticConverter(ITransliterator transliterator, IPhoneticEncoder phoneticEncoder)
    {
        _transliterator = transliterator;
        _phoneticEncoder = phoneticEncoder ?? throw new ArgumentNullException(nameof(phoneticEncoder));
    }

    public string ConvertToPhonetic(string input)
    {
        if (input == null)
            throw new ArgumentNullException(nameof(input));

        if (string.IsNullOrEmpty(input))
            return string.Empty;

        string transliterated = _transliterator?.Transliterate(input) ?? input;
        return _phoneticEncoder.Encode(transliterated);
    }

    public int MatchRating(string input1, string input2)
    {
        if (input1 == null)
            throw new ArgumentNullException(nameof(input1));

        if (input2 == null)
            throw new ArgumentNullException(nameof(input2));

        if (string.IsNullOrEmpty(input1) && string.IsNullOrEmpty(input2))
            return 5;

        if (string.IsNullOrEmpty(input1) || string.IsNullOrEmpty(input2))
            return 0;

        return _phoneticEncoder.MatchRating(input1, input2);
    }

    public bool IsSimilar(string input1, string input2)
    {
        if (input1 == null)
            throw new ArgumentNullException(nameof(input1));

        if (input2 == null)
            throw new ArgumentNullException(nameof(input2));

        if (string.IsNullOrEmpty(input1) && string.IsNullOrEmpty(input2))
            return true;

        if (string.IsNullOrEmpty(input1) || string.IsNullOrEmpty(input2))
            return false;

        return _phoneticEncoder.IsSimilar(input1, input2);
    }

    public int MinimumRating(string input1, string input2)
    {
        if (input1 == null)
            throw new ArgumentNullException(nameof(input1));

        if (input2 == null)
            throw new ArgumentNullException(nameof(input2));

        int sum = input1.Length + input2.Length;
        if (sum <= 4)
        {
            return 5;
        }

        if (sum <= 7)
        {
            return 4;
        }

        if (sum <= 11)
        {
            return 3;
        }

        if (sum == 12)
        {
            return 2;
        }

        return 0;
    }
}
