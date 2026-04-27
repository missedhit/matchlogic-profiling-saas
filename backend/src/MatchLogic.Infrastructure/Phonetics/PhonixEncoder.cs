using MatchLogic.Application.Interfaces.Phonetics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Phonix;
using Phonix.Encoding;
using Ardalis.Result;

namespace MatchLogic.Infrastructure.Phonetics;
public class PhonixEncoder : IPhoneticEncoder
{
    private const int _maxKeyLength = 25;
    public string Encode(string input)
    {
        return new Phonix.DoubleMetaphone(_maxKeyLength).BuildKey(input);
    }

    public int MatchRating(string input1, string input2)
    {
        var encoder = new Phonix.MatchRatingApproach();
        var result = encoder.MatchRatingCompute(input1, input2);
        return result;
    }

    public bool IsSimilar(string input1, string input2)
    {
        return new Phonix.DoubleMetaphone(_maxKeyLength).IsSimilar(new string[] { input1, input2 });
    }
}
